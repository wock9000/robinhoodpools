"""Read-only on-chain market data for the LP workbench.

The catalog starts from the public chain registry and discovers pools from
verified factory events. It reports historical gaps while its bounded on-chain
backfill warms instead of treating missing coverage as inactivity or zero TVL.

Selected pools are read directly from a block-pinned RPC snapshot.  A single
bounded worker follows heads and pool logs for at most ``MAX_SELECTIONS``
recent selections; request handlers only seed a selection or copy its cached
snapshot.  No key material is read and this module has no transaction path.
"""
from __future__ import annotations

from collections import OrderedDict, deque
from copy import deepcopy
from dataclasses import dataclass, field, replace
from decimal import Decimal, localcontext
import json
import math
import os
from pathlib import Path
import re
import threading
import time
from typing import Any, Iterable, Mapping, Sequence
import urllib.error
import urllib.request
from urllib.parse import urlsplit, urlunsplit

from eth_utils import keccak

from .lp_math import MAX_TICK, principal_raw, sqrt_ratio_at_tick
from .lp_chain import CHAIN_ID
from . import _mc
from .lp_market_protocols import resolve_v4_tick_spacing



POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
STATE_VIEW = "0xf3334192d15450cdd385c8b70e03f9a6bd9e673b"
UNISWAP_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
# Public chain registry. Keep event families
# explicit: similarly named PoolCreated events do not have interchangeable ABI.
V2_FACTORIES = frozenset({
    "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f",
    "0xdaa80fe4ee10de529d98ea4bde15bcaf7f7324b3",
    "0x0d1ebb179cdbca88d74c923c4255cb2b17474afd",
})
SLIPSTREAM_FACTORY = "0x1ac9db4a2608ba45d6127b1737949b51bb54b7f3"
V3_FACTORIES = frozenset({
    UNISWAP_V3_FACTORY,
    "0xece6ecd61177336ea6fb9b17937ac439d85ee20b",
    "0x0fbfcf9fa4f9c56b0f40a671ad40e0805a091865",
})
CONCENTRATED_FACTORIES = frozenset({SLIPSTREAM_FACTORY, *V3_FACTORIES})
CREATION_EMITTERS = frozenset({
    POOL_MANAGER, SLIPSTREAM_FACTORY, *V2_FACTORIES, *V3_FACTORIES,
})
NATIVE = "0x" + "0" * 40
USDG = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"

Q96 = 1 << 96
Q192 = 1 << 192
BOARD_REFRESH_S = 1.0
HEAD_REFRESH_S = 0.5
CENSUS_REFRESH_S = 15.0
DISCOVERY_REFRESH_S = 0.5
SELECT_REFRESH_S = 0.35
SELECTION_TTL_S = 90.0
SELECTION_ACTIVE_S = 2.0
SELECTION_PREEMPT_S = 0.5
MAX_STATE_MULTICALL_CALLS = 800
MAX_SELECTIONS = 16
MAX_CATCHUP_BLOCKS = 512
SWAP_LOOKBACK_BLOCKS = 512
MAX_SWAP_LOOKBACK_BLOCKS = 4096
SELECT_LOG_CHUNK = 128
MAX_SWAPS = 2048
MAX_TRACKING_POINTS = 512
PARTICIPANT_SCAN_CHUNK = 2_000
PARTICIPANT_SCAN_INTERVAL_S = 2.0
MAX_POSITION_RANGES = 512
MAX_PARTICIPANT_RANGES = 2_048
MAX_PARTICIPANT_REFRESH_PER_HEAD = 64
MAX_LP_EVENTS = 1_024
MAX_BITMAP_WORDS = 8
MAX_CURVE_POINTS = 800
MAX_TOKEN_RESOLVES_PER_PAGE = 200
BACKFILL_INITIAL_CHUNK = 250_000
BACKFILL_MIN_CHUNK = 10_000
BACKFILL_ACTIVE_SELECTION_CHUNK = 2_000
BACKFILL_MAX_CHUNK = 500_000
BACKFILL_CHECKPOINT_S = 5.0
BACKFILL_PAUSE_S = 0.5
INDEX_CHECKPOINT_VERSION = 1
MAX_RPC_RESPONSE_BYTES = 64 * 1024 * 1024
CURRENT_BLOCK_CACHE = max(MAX_CATCHUP_BLOCKS * 2, MAX_SWAP_LOOKBACK_BLOCKS)

_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}\Z")
_BYTES32_RE = re.compile(r"0x[0-9a-f]{64}\Z")


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


def _topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


TOKEN0_SELECTOR = "0x0dfe1681"
TOKEN1_SELECTOR = "0xd21220a7"
FEE_SELECTOR = "0xddca3f43"
FACTORY_SELECTOR = "0xc45a0155"
GET_POOL_SELECTOR = _selector("getPool(address,address,uint24)")
TICK_SPACING_SELECTOR = "0xd0c93a7c"
SLOT0_SELECTOR = "0x3850c7bd"
LIQUIDITY_SELECTOR = "0x1a686502"
RESERVES_SELECTOR = "0x0902f1ac"
TOTAL_SUPPLY_SELECTOR = "0x18160ddd"
BALANCE_OF_SELECTOR = "0x70a08231"
SYMBOL_SELECTOR = "0x95d89b41"
DECIMALS_SELECTOR = "0x313ce567"
POSITIONS_SELECTOR = _selector("positions(bytes32)")
TICK_BITMAP_SELECTOR = _selector("tickBitmap(int16)")
TICKS_SELECTOR = _selector("ticks(int24)")
FEE_GROWTH_GLOBAL0_SELECTOR = _selector("feeGrowthGlobal0X128()")
FEE_GROWTH_GLOBAL1_SELECTOR = _selector("feeGrowthGlobal1X128()")
SV_SLOT0_SELECTOR = _selector("getSlot0(bytes32)")
SV_LIQUIDITY_SELECTOR = _selector("getLiquidity(bytes32)")
SV_TICK_BITMAP_SELECTOR = _selector("getTickBitmap(bytes32,int16)")
SV_TICK_INFO_SELECTOR = _selector("getTickInfo(bytes32,int24)")

V3_SWAP_TOPIC = _topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
V3_MINT_TOPIC = _topic("Mint(address,address,int24,int24,uint128,uint256,uint256)")
V3_BURN_TOPIC = _topic("Burn(address,int24,int24,uint128,uint256,uint256)")
V3_COLLECT_TOPIC = _topic("Collect(address,address,int24,int24,uint128,uint128)")
V4_SWAP_TOPIC = _topic("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")
V4_MODIFY_TOPIC = _topic("ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)")
V2_SWAP_TOPIC = _topic("Swap(address,uint256,uint256,uint256,uint256,address)")
V2_SYNC_TOPIC = _topic("Sync(uint112,uint112)")
V4_INITIALIZE_TOPIC = _topic("Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)")
V3_POOL_CREATED_TOPIC = _topic("PoolCreated(address,address,uint24,int24,address)")
V3_POOL_CREATED_COMPACT_TOPIC = _topic("PoolCreated(address,address,uint24,address)")
SLIPSTREAM_POOL_CREATED_TOPIC = _topic("PoolCreated(address,address,int24,address)")
V2_PAIR_CREATED_TOPIC = _topic("PairCreated(address,address,address,uint256)")
CREATION_TOPICS = (
    V4_INITIALIZE_TOPIC,
    V3_POOL_CREATED_TOPIC,
    V3_POOL_CREATED_COMPACT_TOPIC,
    SLIPSTREAM_POOL_CREATED_TOPIC,
    V2_PAIR_CREATED_TOPIC,
)


class MarketError(RuntimeError):
    """The market service could not produce an authoritative live value."""


class RpcError(MarketError):
    """A JSON-RPC request failed or returned malformed data."""


class ReorgDetected(MarketError):
    """A selected-pool cursor is no longer on the canonical chain."""


def _hex_int(value: Any) -> int:
    if isinstance(value, bool):
        raise ValueError("boolean is not an integer")
    if isinstance(value, int):
        return value
    return int(str(value), 16)


def _words(value: Any) -> list[str]:
    text = str(value or "")
    if not text.startswith("0x") or (len(text) - 2) % 64:
        raise ValueError("malformed ABI data")
    body = text[2:]
    return [body[index : index + 64] for index in range(0, len(body), 64)]


def _signed_word(word: str, bits: int = 256) -> int:
    value = int(word, 16)
    mask = (1 << bits) - 1
    value &= mask
    return value - (1 << bits) if value >= (1 << (bits - 1)) else value


def _signed_topic(topic: str) -> int:
    return _signed_word(str(topic)[2:].rjust(64, "0"))


def _encode_signed(value: int) -> str:
    return f"{value % (1 << 256):064x}"


def _address_word(value: Any) -> str:
    words = _words(value)
    if not words:
        raise ValueError("address result is empty")
    address = "0x" + words[0][-40:].lower()
    if not _ADDRESS_RE.fullmatch(address):
        raise ValueError("malformed address result")
    return address
def _topic_address(value: Any) -> str:
    text = str(value or "").lower()
    if not text.startswith("0x") or len(text) != 66:
        raise ValueError("malformed indexed address")
    address = "0x" + text[-40:]
    if not _ADDRESS_RE.fullmatch(address):
        raise ValueError("malformed indexed address")
    return address


def _word_address(word: str) -> str:
    address = "0x" + word[-40:].lower()
    if not _ADDRESS_RE.fullmatch(address):
        raise ValueError("malformed ABI address")
    return address






def _position_key(owner: str, lo: int, hi: int) -> str:
    packed = bytes.fromhex(owner[2:]) + lo.to_bytes(3, "big", signed=True) + hi.to_bytes(3, "big", signed=True)
    return keccak(packed).hex()


def _position_call(owner: str, lo: int, hi: int) -> str:
    return POSITIONS_SELECTOR + _position_key(owner, lo, hi).rjust(64, "0")


def _balance_call(owner: str) -> str:
    return BALANCE_OF_SELECTOR + owner[2:].rjust(64, "0")


def _v4_configured_fee(pool: "_Pool") -> int | None:
    if pool.fee_ppm is not None:
        return int(pool.fee_ppm)
    return 0x800000 if pool.dynamic_fee else None


def _v4_pool_key(pool: "_Pool") -> bytes:
    configured_fee = _v4_configured_fee(pool)
    if configured_fee is None:
        raise ValueError("V4 PoolKey is missing its configured fee")
    return b"".join((
        bytes.fromhex(pool.token0[2:].rjust(64, "0")),
        bytes.fromhex(pool.token1[2:].rjust(64, "0")),
        configured_fee.to_bytes(32, "big"),
        int(pool.tick_spacing or 0).to_bytes(32, "big", signed=True),
        bytes.fromhex((pool.hook or NATIVE)[2:].rjust(64, "0")),
    ))


def _v4_pool_id(pool: "_Pool") -> str:
    return "0x" + keccak(_v4_pool_key(pool)).hex()


def _resolve_v4_pool_key(pool: "_Pool") -> "_Pool":
    configured_fee = _v4_configured_fee(pool)
    if configured_fee is None:
        raise RpcError("V4 census PoolKey is missing its configured fee")
    spacing = resolve_v4_tick_spacing(
        pool_id=pool.id,
        currency0=pool.token0,
        currency1=pool.token1,
        fee=configured_fee,
        hooks=pool.hook or NATIVE,
        tick_spacing=pool.tick_spacing,
    )
    if spacing is None:
        raise RpcError("V4 census PoolKey does not hash to the selected pool id")
    return pool if pool.tick_spacing == spacing else replace(pool, tick_spacing=spacing)


def _decode_symbol(value: Any) -> str | None:
    try:
        words = _words(value)
        if not words:
            return None
        if int(words[0], 16) == 32 and len(words) >= 2:
            length = min(int(words[1], 16), 96)
            packed = bytes.fromhex("".join(words[2:]))[:length]
        else:
            packed = bytes.fromhex(words[0]).rstrip(b"\0")
        text = packed.decode("utf-8", errors="replace").strip()
        text = "".join(char for char in text if char.isprintable())[:32]
        return text or None
    except (ValueError, UnicodeError):
        return None


def _format_units(raw: int, decimals: int | None) -> str:
    """Format an integer exactly, without a float or scientific notation."""
    if decimals is None:
        return str(raw)
    sign = "-" if raw < 0 else ""
    digits = str(abs(raw)).rjust(decimals + 1, "0")
    if decimals == 0:
        return sign + digits
    whole, fraction = digits[:-decimals], digits[-decimals:]
    fraction = fraction.rstrip("0")
    return sign + whole + ("." + fraction if fraction else "")


def _price_from_sqrt(sqrt_price_x96: int, decimals0: int | None, decimals1: int | None) -> float | None:
    if sqrt_price_x96 <= 0 or decimals0 is None or decimals1 is None:
        return None
    with localcontext() as context:
        context.prec = 72
        price = (
            Decimal(sqrt_price_x96) * Decimal(sqrt_price_x96) / Decimal(Q192)
            * (Decimal(10) ** (decimals0 - decimals1))
        )
        result = float(price)
    return result if math.isfinite(result) and result > 0 else None


def _tick_price(tick: int, decimals0: int | None, decimals1: int | None) -> float | None:
    return _price_from_sqrt(sqrt_ratio_at_tick(tick), decimals0, decimals1)


def _usd_price(token0: str, token1: str, token1_per_token0: float | None) -> float | None:
    if token1_per_token0 is None:
        return None
    if token1 == USDG and token0 != USDG:
        return token1_per_token0
    if token0 == USDG and token1 != USDG:
        return 1.0 / token1_per_token0 if token1_per_token0 else None
    return None
_UINT256_MASK = (1 << 256) - 1


def _v3_fee_claim(
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
    below0 = lower_outside0 if tick >= lower else (global0 - lower_outside0) & _UINT256_MASK
    below1 = lower_outside1 if tick >= lower else (global1 - lower_outside1) & _UINT256_MASK
    above0 = upper_outside0 if tick < upper else (global0 - upper_outside0) & _UINT256_MASK
    above1 = upper_outside1 if tick < upper else (global1 - upper_outside1) & _UINT256_MASK
    inside0 = (global0 - below0 - above0) & _UINT256_MASK
    inside1 = (global1 - below1 - above1) & _UINT256_MASK
    lazy0 = liquidity * ((inside0 - fee_growth0_last) & _UINT256_MASK) // (1 << 128)
    lazy1 = liquidity * ((inside1 - fee_growth1_last) & _UINT256_MASK) // (1 << 128)
    return owed0 + lazy0, owed1 + lazy1, lazy0, lazy1


def _interval_amounts(
    accounting: dict[str, Any],
    principal0: int,
    principal1: int,
    claim0: int,
    claim1: int,
) -> tuple[int, int, int, int]:
    """Return earned fees and net token changes, not dollar-denominated PnL.

    Burn amounts move principal into ``tokensOwed`` and cancel from earned
    fees. Dollar PnL separately preserves baseline and cashflow-time marks.
    """
    fee0 = (
        claim0
        + int(accounting["collected0"])
        - int(accounting["burn_principal0"])
        - int(accounting["baseline_claim0"])
    )
    fee1 = (
        claim1
        + int(accounting["collected1"])
        - int(accounting["burn_principal1"])
        - int(accounting["baseline_claim1"])
    )
    net0 = (
        principal0
        + claim0
        + int(accounting["collected0"])
        - int(accounting["deposited0"])
        - int(accounting["baseline_equity0"])
    )
    net1 = (
        principal1
        + claim1
        + int(accounting["collected1"])
        - int(accounting["deposited1"])
        - int(accounting["baseline_equity1"])
    )
    return fee0, fee1, net0, net1


def _safe_number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None

def _history_error_is_failure(error: str | None) -> bool:
    return bool(
        error
        and not str(error).startswith("recent event history is ")
    )


class _Rpc:
    """Serialized, bounded stdlib JSON-RPC client shared by one service."""

    def __init__(self, url: str, timeout: float = 4.0) -> None:
        parsed = urlsplit(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ValueError("rpc_url must be an http(s) URL")
        self.url = url
        self.timeout = timeout
        self._lock = threading.Lock()
        self._request_id = 0
        self.closed = False

    def _post(self, payload: dict[str, Any] | list[dict[str, Any]]) -> Any:
        body = json.dumps(payload, separators=(",", ":")).encode()
        request = urllib.request.Request(
            self.url,
            data=body,
            headers={"Content-Type": "application/json", "User-Agent": "deepstate-workbench-market/1"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if response.status != 200:
                    raise RpcError(f"RPC HTTP {response.status}")
                raw = response.read(MAX_RPC_RESPONSE_BYTES + 1)
                if len(raw) > MAX_RPC_RESPONSE_BYTES:
                    raise RpcError(f"RPC response exceeds {MAX_RPC_RESPONSE_BYTES} bytes")
                return json.loads(raw)
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            raise RpcError(str(exc)) from exc

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        try:
            with self._lock:
                if self.closed:
                    raise RpcError("RPC client is closed")
                self._request_id += 1
                request_id = self._request_id
                response = self._post({
                    "jsonrpc": "2.0", "id": request_id, "method": method, "params": params or [],
                })
        except RpcError as exc:
            raise RpcError(f"{method}: {exc}") from exc
        if not isinstance(response, dict) or response.get("id") != request_id:
            raise RpcError(f"malformed response for {method}")
        if response.get("error") is not None:
            raise RpcError(f"{method}: {response['error']}")
        return response.get("result")

    def batch(self, calls: Iterable[tuple[str, list[Any]]]) -> list[Any]:
        specifications = list(calls)
        if not specifications:
            return []
        method_context = ",".join(dict.fromkeys(method for method, _params in specifications))
        try:
            with self._lock:
                if self.closed:
                    raise RpcError("RPC client is closed")
                requests = []
                ids = []
                for method, params in specifications:
                    self._request_id += 1
                    ids.append(self._request_id)
                    requests.append({
                        "jsonrpc": "2.0", "id": self._request_id, "method": method, "params": params,
                    })
                response = self._post(requests)
        except RpcError as exc:
            raise RpcError(f"batch[{method_context}]: {exc}") from exc
        if not isinstance(response, list):
            raise RpcError("batch RPC returned a non-list")
        by_id = {item.get("id"): item for item in response if isinstance(item, dict)}
        output = []
        for request_id, (method, _params) in zip(ids, specifications):
            item = by_id.get(request_id)
            if item is None:
                raise RpcError(f"batch RPC omitted {method}")
            if item.get("error") is not None:
                raise RpcError(f"{method}: {item['error']}")
            output.append(item.get("result"))
        return output

    def close(self) -> None:
        with self._lock:
            self.closed = True


@dataclass(frozen=True, slots=True)
class _Pool:
    id: str
    address: str
    kind: str
    token0: str
    token1: str
    fee_ppm: int | None
    tick_spacing: int | None = None
    hook: str | None = None
    dynamic_fee: bool = False
    factory: str | None = None
    created_block: int | None = None
    source: str = "census"


@dataclass(slots=True)
class _Token:
    symbol: str | None = None
    decimals: int | None = None
    source: str | None = None


@dataclass
class _Universe:
    pools: tuple[_Pool, ...]
    by_id: dict[str, _Pool]
    counts: dict[str, int]
    sources: tuple[dict[str, Any], ...]
    errors: tuple[str, ...]
    tokens: dict[str, _Token]


_UNIVERSE_LOCK = threading.Lock()
_UNIVERSE_CACHE: dict[tuple[Any, ...], _Universe] = {}


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _path_label(path: Path) -> str:
    return path.name


def _load_universe() -> _Universe:
    """Start from public registry tokens; factories are discovered on chain."""
    tokens = {
        NATIVE: _Token("ETH", 18, "public chain registry"),
        USDG: _Token("USDG", 6, "public chain registry"),
    }
    return _Universe((), {}, {"v2": 0, "v3": 0, "v4": 0}, (), (), tokens)


@dataclass
class _Selection:
    pool: _Pool
    owner: str | None
    lock: threading.RLock = field(default_factory=threading.RLock)
    core: dict[str, Any] | None = None
    core_block: int | None = None
    core_hash: str | None = None
    snapshot_revision: int = 0
    created_at: float = field(default_factory=time.time)
    snapshot: dict[str, Any] | None = None
    cursor: int = 0
    cursor_hash: str = ""
    last_used: float = field(default_factory=time.monotonic)
    last_refresh: float = 0.0
    last_head_change: float = 0.0
    needs_seed: bool = True
    ranges: dict[tuple[int, int], int] = field(default_factory=dict)
    position_state: dict[tuple[int, int], tuple[int, int, int]] = field(default_factory=dict)
    owner_range_seed_block: int | None = None
    participant_ranges: dict[tuple[str, int, int], int] = field(default_factory=dict)
    participant_state: dict[tuple[str, int, int], dict[str, int]] = field(default_factory=dict)
    participant_tick_fees: dict[int, tuple[int, int]] = field(default_factory=dict)
    participant_kinds: dict[str, str] = field(default_factory=dict)
    participant_accounting: dict[tuple[str, int, int], dict[str, Any]] = field(
        default_factory=dict
    )
    participant_ranges_truncated: bool = False
    participant_refresh_error: str | None = None
    participant_refresh_block: int | None = None
    accounting_generation: int = 0
    accounting_reset_reason: str | None = None
    lp_events: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=MAX_LP_EVENTS)
    )
    lp_event_coverage_start_block: int | None = None
    lp_event_coverage_start_timestamp: int | None = None
    lp_events_truncated: bool = False
    history_floor: int = 0
    history_complete: bool = False
    history_generation: int = 0
    history_frontier_hash: str | None = None
    history_error: str | None = None
    last_history_scan: float = 0.0
    last_recent_history_scan: float = 0.0
    recent_history_pending: bool = False
    recent_history_cursor: int = 0
    swaps: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=MAX_SWAPS))
    swaps_truncated_through_timestamp: int | None = None
    swap_coverage_start_block: int | None = None
    swap_coverage_start_timestamp: int | None = None
    swap_coverage_through_block: int | None = None
    swap_coverage_through_hash: str | None = None
    current_event_start_block: int | None = None
    current_event_start_timestamp: int | None = None
    current_event_through_block: int | None = None
    current_event_through_hash: str | None = None
    current_event_error: str | None = None
    current_observation_epoch: int = 0
    current_event_revisions: OrderedDict[int, int] = field(default_factory=OrderedDict)
    current_event_identities: dict[
        tuple[str, str, int], dict[str, Any]
    ] = field(default_factory=dict)
    owner_liquidity_changes: deque[dict[str, int]] = field(
        default_factory=lambda: deque(maxlen=MAX_SWAPS)
    )
    tracking: deque[dict[str, Any]] = field(
        default_factory=lambda: deque(maxlen=MAX_TRACKING_POINTS)
    )
    tick_net: dict[int, int] = field(default_factory=dict)
    curve_words: tuple[int, ...] = ()
    curve_state_block: int | None = None
    curve_state_hash: str | None = None
    curve_verified_through_block: int | None = None
    curve_dirty_from_block: int | None = None
    snapshot_dirty: bool = False
    factory: str | None = None
    factory_member: bool = False
    factory_membership_error: str | None = None
    current_fee: int | None = None
    reorgs: int = 0
    last_reorg: str | None = None
    ranges_truncated: bool = False
    position_error: str | None = None
    refresh_failures: int = 0
    reconnects: int = 0
    disconnected: bool = False


class MarketService:
    """Full census catalog plus bounded, block-pinned selected-pool state."""

    def __init__(
        self, rpc_url: str, *, data_dir: str | Path, external_index: bool = False,
        rpc: Any | None = None,
    ) -> None:
        self.rpc_url = rpc_url
        self.data_dir = Path(data_dir).expanduser()
        self.index_checkpoint = self.data_dir / "workbench_pool_index.json"
        self._external_index = external_index
        if rpc is None:
            from .lp_rpc import build_rpc_factory
            rpc = build_rpc_factory(rpc_url, RpcError)
        if callable(rpc) and not hasattr(rpc, "call"):
            self.rpc = rpc("workbench")
            self._backfill_rpc = rpc("backfill")
            self._maintenance_rpc = rpc("maintenance")
        else:
            self.rpc = self._backfill_rpc = self._maintenance_rpc = rpc
        self._lock = threading.RLock()
        self._detail_cond = threading.Condition(self._lock)
        self._detail_revision = 0
        self.universe = _load_universe()
        self._census_checked_mono = 0.0
        self._census_checked_at = time.time()
        self._census_reloaded_at = time.time()
        self._census_reload_error: str | None = None
        self._census_generation = 1
        self._discovered: dict[str, _Pool] = {}
        self._invalid_index_pools: set[str] = set()
        self._index_publication_revision = 0
        self._discovery_last_poll = 0.0
        self._discovery_started_block: int | None = None
        self._discovery_started_hash: str | None = None
        self._discovery_started_at: float | None = None
        self._discovery_cursor = 0
        self._discovery_cursor_hash = ""
        self._discovery_error: str | None = "live factory tail not anchored"
        self._discovery_reorgs = 0
        self._discovery_last_reorg: str | None = None
        self._backfill_lanes: dict[str, dict[str, Any]] = {}
        self._backfill_error: str | None = None
        self._backfill_retry_after = 0.0
        self._backfill_chain_verified = False
        self._maintenance_chain_verified = False
        self._checkpoint_error: str | None = None
        self._checkpoint_loaded_at: float | None = None
        self._checkpoint_written_at: float | None = None
        self._checkpoint_last_write = 0.0
        self._checkpoint_dirty = False
        self._checkpoint_revision = 0
        self._checkpoint_lock = threading.Lock()
        self._board: dict[str, dict[str, Any]] = {}
        self._board_health: dict[str, Any] = {"state": "disabled", "error": None}
        self._board_as_of = 0.0
        self._board_read_at = 0.0
        self._head: dict[str, Any] | None = None
        self._head_read_at = 0.0
        self._head_error: str | None = None
        self._chain_verified = False
        self._current_blocks: OrderedDict[int, dict[str, Any]] = OrderedDict()
        self._current_observation_epoch = 1
        self._current_observation_error: str | None = None
        self._token_retry_after: dict[str, float] = {}
        self._selected: OrderedDict[tuple[str, str | None], _Selection] = OrderedDict()
        self._catalog_cache: OrderedDict[tuple[Any, ...], dict[str, Any]] = OrderedDict()
        self._page_metadata: OrderedDict[str, _Pool] = OrderedDict()
        if not self._external_index:
            self._load_index_checkpoint()
        self._stop = threading.Event()
        self._selection_wake = threading.Event()
        self._thread = threading.Thread(target=self._run, name="workbench-market", daemon=True)
        self._backfill_thread = threading.Thread(
            target=self._backfill_run,
            name="workbench-market-index",
            daemon=True,
        )
        self._maintenance_thread = threading.Thread(
            target=self._maintenance_run,
            name="workbench-market-maintenance",
            daemon=True,
        )
        self._thread.start()
        if not self._external_index:
            self._backfill_thread.start()
        self._maintenance_thread.start()

    def close(self) -> None:
        self._stop.set()
        self._selection_wake.set()
        with self._detail_cond:
            self._detail_cond.notify_all()
        current = threading.current_thread()
        if self._thread is not current:
            self._thread.join(timeout=10.0)
        if not self._external_index and self._backfill_thread is not current:
            self._backfill_thread.join(timeout=25.0)
        if self._maintenance_thread is not current:
            self._maintenance_thread.join(timeout=25.0)
        if not self._external_index:
            self._write_index_checkpoint(force=True)
        self.rpc.close()
        self._backfill_rpc.close()
        self._maintenance_rpc.close()

    def __enter__(self) -> "MarketService":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.close()

    @staticmethod
    def _normalize_current_header(header: Mapping[str, Any]) -> dict[str, Any] | None:
        try:
            number = _hex_int(header.get("number"))
            timestamp = _hex_int(header.get("timestamp"))
        except (TypeError, ValueError):
            return None
        block_hash = str(header.get("hash") or "").lower()
        parent_hash = str(
            header.get("parentHash") or header.get("parent_hash") or ""
        ).lower()
        if (
            number < 0
            or timestamp < 0
            or not _BYTES32_RE.fullmatch(block_hash)
            or not _BYTES32_RE.fullmatch(parent_hash)
        ):
            return None
        return {
            **dict(header),
            "number": hex(number),
            "hash": block_hash,
            "parentHash": parent_hash,
            "timestamp": hex(timestamp),
        }

    def observe_current_block(self, header: Mapping[str, Any]) -> None:
        """Accept one already-verified head without doing network work."""
        current = self._normalize_current_header(header)
        if current is None:
            return
        number = _hex_int(current["number"])
        with self._lock:
            latest_number = next(reversed(self._current_blocks), None)
            latest = (
                self._current_blocks.get(latest_number)
                if latest_number is not None else None
            )
            existing = self._current_blocks.get(number)
            if existing is not None and existing["hash"] == current["hash"]:
                return
            if latest_number is not None and number < latest_number:
                return
            discontinuity: str | None = None
            if latest is not None and number == latest_number:
                discontinuity = f"same-height block {number} was replaced"
            elif latest is not None and number != latest_number + 1:
                discontinuity = (
                    f"current event feed skipped blocks {latest_number + 1}..{number - 1}"
                )
            elif latest is not None and current["parentHash"] != latest["hash"]:
                discontinuity = (
                    f"current block {number} parent does not match observed block "
                    f"{latest_number}"
                )
            if discontinuity is not None:
                self._current_blocks.clear()
                self._current_observation_epoch += 1
                self._current_observation_error = discontinuity
            self._current_blocks[number] = {
                "number": number,
                "hash": current["hash"],
                "parent_hash": current["parentHash"],
                "timestamp": _hex_int(current["timestamp"]),
                "events_observed": False,
                "events": {},
                "revision": 0,
            }
            self._current_blocks.move_to_end(number)
            while len(self._current_blocks) > CURRENT_BLOCK_CACHE:
                self._current_blocks.popitem(last=False)
            cached_head = _hex_int(self._head["number"]) if self._head else -1
            if number >= cached_head:
                self._head = current
                self._head_error = None
                self._head_read_at = time.monotonic()
        self._selection_wake.set()

    def observe_current_events(
        self,
        header: Mapping[str, Any],
        events: Sequence[Mapping[str, Any]],
    ) -> None:
        """Merge verified observed rows or later enrichment; absence is not coverage."""
        current = self._normalize_current_header(header)
        if current is None:
            return
        number = _hex_int(current["number"])
        normalized: list[tuple[tuple[str, str, int], dict[str, Any]]] = []
        try:
            for event in events:
                block = int(event.get("block_number"))
                block_hash = str(event.get("block_hash") or "").lower()
                tx_hash = str(event.get("tx_hash") or "").lower()
                log_index = int(event.get("log_index"))
                timestamp = int(event.get("timestamp"))
                if (
                    block != number
                    or block_hash != current["hash"]
                    or timestamp != _hex_int(current["timestamp"])
                    or not _BYTES32_RE.fullmatch(block_hash)
                    or not _BYTES32_RE.fullmatch(tx_hash)
                    or log_index < 0
                ):
                    return
                normalized.append(
                    ((block_hash, tx_hash, log_index), dict(event))
                )
        except (AttributeError, TypeError, ValueError):
            return
        with self._lock:
            block_state = self._current_blocks.get(number)
            if block_state is None or block_state["hash"] != current["hash"]:
                return
            for identity, event in normalized:
                pool_id = self._current_event_pool_id(event)
                if not pool_id:
                    continue
                pool_events = block_state["events"].setdefault(pool_id, {})
                previous = pool_events.get(identity)
                merged = {**previous, **event} if previous is not None else event
                if previous != merged:
                    # Published row objects are replaced, never mutated.
                    pool_events[identity] = merged
                    block_state["revision"] += 1
            block_state["events_observed"] = True
        self._selection_wake.set()

    def source_status(self) -> dict[str, Any]:
        status = getattr(self.rpc, "status", None)
        return dict(status()) if callable(status) else {}

    def _pool_by_id(self, pool_id: str) -> _Pool | None:
        normalized = str(pool_id or "").strip().lower()
        with self._lock:
            if normalized in self._invalid_index_pools:
                return None
            return self._discovered.get(normalized) or self.universe.by_id.get(normalized)

    @property
    def pool_publication_revision(self) -> int:
        with self._lock:
            return self._index_publication_revision

    def wait_pool(self, pool_id: str, timeout: float = 2.0) -> _Pool | None:
        """Wait for the bounded index resolver to publish one cold identity."""
        normalized = str(pool_id or "").strip().lower()
        deadline = time.monotonic() + max(0.0, timeout)
        with self._detail_cond:
            while True:
                pool = self._pool_by_id(normalized)
                if pool is not None:
                    return pool
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                self._detail_cond.wait(remaining)

    def register_index_pool(self, value: dict[str, Any]) -> None:
        metadata = value.get("metadata_json") or {}
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        if not isinstance(metadata, Mapping):
            metadata = {}
        pool = _Pool(
            value["id"], value["address"], value["protocol"], value["token0"], value["token1"],
            value.get("fee_ppm"), value.get("tick_spacing"), value.get("hook"),
            bool(metadata.get("dynamic_fee")),
            value.get("factory"),
            value.get("created_block"), value.get("source") or "durable-index",
        )
        if pool.kind == "v4":
            try:
                pool = _resolve_v4_pool_key(pool)
            except RpcError as exc:
                raise ValueError(
                    "durable V4 PoolKey does not hash to its pool id"
                ) from exc
        metadata_changed = False
        with self._lock:
            self._invalid_index_pools.discard(pool.id)
            previous = self._discovered.get(pool.id) or self.universe.by_id.get(pool.id)
            if previous != pool:
                self._discovered[pool.id] = pool
            for side in (0, 1):
                address = value[f"token{side}"]
                token = self.universe.tokens.setdefault(address, _Token())
                symbol, decimals = value.get(f"symbol{side}"), value.get(f"decimals{side}")
                if symbol is not None and token.symbol != symbol:
                    token.symbol = str(symbol)
                    metadata_changed = True
                if decimals is not None and token.decimals != int(decimals):
                    token.decimals = int(decimals)
                    metadata_changed = True
                if (symbol is not None or decimals is not None) and token.source != "durable-index":
                    token.source = "durable-index"
                    metadata_changed = True
            changed = previous != pool or metadata_changed
            selections = [
                selection for selection in self._selected.values()
                if selection.pool.id == pool.id
            ] if changed else []
            if changed:
                self._index_publication_revision += 1
                self._catalog_cache.clear()
            self._detail_cond.notify_all()
        for selection in selections:
            with selection.lock:
                selection.pool = pool
                selection.snapshot_dirty = True
        if selections:
            self._selection_wake.set()

    def unregister_index_pools(self, pool_ids: Iterable[str]) -> None:
        """Invalidate orphaned pool discoveries and their selected snapshots."""
        removed = {str(pool_id).lower() for pool_id in pool_ids}
        if not removed:
            return
        with self._lock:
            self._invalid_index_pools.update(removed)
            for pool_id in removed:
                self._discovered.pop(pool_id, None)
            for key in list(self._selected):
                if key[0] in removed:
                    self._selected.pop(key, None)
            self._index_publication_revision += 1
            self._catalog_cache.clear()
            self._detail_cond.notify_all()

    @staticmethod
    def _pool_metadata_agrees(left: _Pool, right: _Pool) -> bool:
        return (
            left.kind == right.kind
            and left.token0 == right.token0
            and left.token1 == right.token1
            and (
                left.fee_ppm is None
                or right.fee_ppm is None
                or left.fee_ppm == right.fee_ppm
            )
            and (
                left.tick_spacing is None
                or right.tick_spacing is None
                or left.tick_spacing == right.tick_spacing
            )
            and (
                left.kind != "v4"
                or left.hook is None
                or right.hook is None
                or left.hook == right.hook
            )
        )
    @staticmethod
    def _checkpoint_pool(pool: _Pool) -> dict[str, Any]:
        return {
            "id": pool.id,
            "address": pool.address,
            "kind": pool.kind,
            "token0": pool.token0,
            "token1": pool.token1,
            "fee_ppm": pool.fee_ppm,
            "tick_spacing": pool.tick_spacing,
            "hook": pool.hook,
            "dynamic_fee": pool.dynamic_fee,
            "factory": pool.factory,
            "created_block": pool.created_block,
            "source": pool.source,
        }

    @staticmethod
    def _pool_from_checkpoint(value: Any) -> _Pool:
        if not isinstance(value, dict):
            raise ValueError("checkpoint pool is not an object")
        kind = str(value.get("kind") or "")
        if kind not in {"v2", "v3", "v4"}:
            raise ValueError("checkpoint pool kind is unsupported")
        pool_id = str(value.get("id") or "").lower()
        address = str(value.get("address") or "").lower()
        token0 = str(value.get("token0") or "").lower()
        token1 = str(value.get("token1") or "").lower()
        expected_id = _BYTES32_RE if kind == "v4" else _ADDRESS_RE
        if (
            not expected_id.fullmatch(pool_id)
            or not _ADDRESS_RE.fullmatch(address)
            or not _ADDRESS_RE.fullmatch(token0)
            or not _ADDRESS_RE.fullmatch(token1)
        ):
            raise ValueError("checkpoint pool has malformed identifiers")
        if kind != "v4" and address != pool_id:
            raise ValueError("checkpoint pair address does not equal its id")
        fee_value = value.get("fee_ppm")
        spacing_value = value.get("tick_spacing")
        block_value = value.get("created_block")
        fee = int(fee_value) if fee_value is not None else None
        spacing = int(spacing_value) if spacing_value is not None else None
        created_block = int(block_value) if block_value is not None else None
        if fee is not None and not 0 <= fee <= 0xFFFFFF:
            raise ValueError("checkpoint pool fee is out of range")
        if created_block is not None and created_block < 0:
            raise ValueError("checkpoint creation block is negative")
        hook_value = value.get("hook")
        hook = str(hook_value).lower() if hook_value is not None else None
        if hook is not None and not _ADDRESS_RE.fullmatch(hook):
            raise ValueError("checkpoint hook is malformed")
        factory_value = value.get("factory")
        factory = str(factory_value).lower() if factory_value is not None else None
        if factory is not None and not _ADDRESS_RE.fullmatch(factory):
            raise ValueError("checkpoint factory is malformed")
        source = str(value.get("source") or "")
        if source not in {"factory-live", "backfill-v4", "backfill-legacy"}:
            raise ValueError("checkpoint pool source is unsupported")
        pool = _Pool(
            pool_id,
            address,
            kind,
            token0,
            token1,
            fee,
            spacing,
            hook,
            bool(value.get("dynamic_fee")),
            factory,
            created_block,
            source,
        )
        if kind == "v4":
            try:
                pool = _resolve_v4_pool_key(pool)
            except RpcError as exc:
                raise ValueError("checkpoint V4 PoolKey hash mismatch") from exc
        return pool

    @staticmethod
    def _checkpoint_lane(value: Any) -> dict[str, Any]:
        if not isinstance(value, dict):
            raise ValueError("checkpoint backfill lane is not an object")
        floor = int(value.get("floor"))
        next_to = int(value.get("next_to"))
        high_block = int(value.get("high_block"))
        if (
            floor < 0
            or high_block < 0
            or next_to > high_block
            or (floor <= high_block and next_to < floor - 1)
        ):
            raise ValueError("checkpoint backfill frontier is invalid")
        high_hash = str(value.get("high_hash") or "").lower()
        if not _BYTES32_RE.fullmatch(high_hash):
            raise ValueError("checkpoint backfill high hash is invalid")
        low_value = value.get("low_block")
        low_block = int(low_value) if low_value is not None else None
        low_hash_value = value.get("low_hash")
        low_hash = str(low_hash_value).lower() if low_hash_value is not None else None
        if low_block is not None and (low_block < floor or low_block > high_block):
            raise ValueError("checkpoint backfill low block is invalid")
        if low_hash is not None and not _BYTES32_RE.fullmatch(low_hash):
            raise ValueError("checkpoint backfill low hash is invalid")
        return {
            "floor": floor,
            "next_to": next_to,
            "high_block": high_block,
            "high_hash": high_hash,
            "low_block": low_block,
            "low_hash": low_hash,
            "complete": floor > high_block or next_to < floor,
            "chunk": min(
                BACKFILL_MAX_CHUNK,
                max(BACKFILL_MIN_CHUNK, int(value.get("chunk") or BACKFILL_INITIAL_CHUNK)),
            ),
            "blocks_scanned": max(0, int(value.get("blocks_scanned") or 0)),
            "logs_scanned": max(0, int(value.get("logs_scanned") or 0)),
            "new_pools": max(0, int(value.get("new_pools") or 0)),
            "failures": max(0, int(value.get("failures") or 0)),
            "consecutive_failures": 0,
            "error": None,
            "updated_at": float(value.get("updated_at") or time.time()),
        }

    def _load_index_checkpoint(self) -> None:
        try:
            raw = _load_json(self.index_checkpoint)
            if not isinstance(raw, dict):
                raise ValueError("root is not an object")
            if int(raw.get("version") or 0) != INDEX_CHECKPOINT_VERSION:
                raise ValueError("unsupported version")
            if int(raw.get("chain_id") or 0) != CHAIN_ID:
                raise ValueError("wrong chain id")
            pools: dict[str, _Pool] = {}
            for value in raw.get("pools") or []:
                pool = self._pool_from_checkpoint(value)
                census_pool = self.universe.by_id.get(pool.id)
                if census_pool is not None:
                    if not self._pool_metadata_agrees(census_pool, pool):
                        raise ValueError(f"pool {pool.id} conflicts with census")
                    continue
                pools[pool.id] = pool
            lane_values = raw.get("backfill")
            lanes: dict[str, dict[str, Any]] = {}
            if isinstance(lane_values, dict):
                for name in ("v4", "legacy"):
                    if name in lane_values:
                        lanes[name] = self._checkpoint_lane(lane_values[name])
            tail = raw.get("tail") if isinstance(raw.get("tail"), dict) else {}
            start = int(tail.get("start_block") or 0)
            cursor = int(tail.get("cursor") or 0)
            start_hash = str(tail.get("start_hash") or "").lower()
            cursor_hash = str(tail.get("cursor_hash") or "").lower()
            if start or cursor:
                if (
                    start <= 0
                    or cursor < start
                    or not _BYTES32_RE.fullmatch(start_hash)
                    or not _BYTES32_RE.fullmatch(cursor_hash)
                ):
                    raise ValueError("live tail frontier is invalid")
                self._discovery_started_block = start
                self._discovery_started_hash = start_hash
                self._discovery_started_at = float(tail.get("started_at") or time.time())
                self._discovery_cursor = cursor
                self._discovery_cursor_hash = cursor_hash
                self._discovery_error = "persisted live tail awaiting canonical verification"
            self._discovered = pools
            self._backfill_lanes = lanes
            self._checkpoint_loaded_at = time.time()
        except FileNotFoundError:
            return
        except Exception as exc:
            self._checkpoint_error = f"index checkpoint ignored: {exc}"

    def _mark_checkpoint_dirty_locked(self) -> None:
        self._checkpoint_revision += 1
        self._checkpoint_dirty = True

    def _write_index_checkpoint(self, force: bool = False) -> None:
        now_mono = time.monotonic()
        with self._lock:
            if not self._checkpoint_dirty and not force:
                return
            if not force and now_mono - self._checkpoint_last_write < BACKFILL_CHECKPOINT_S:
                return
            revision = self._checkpoint_revision
            pools = tuple(self._discovered.values())
            lanes = deepcopy(self._backfill_lanes)
            payload = {
                "version": INDEX_CHECKPOINT_VERSION,
                "chain_id": CHAIN_ID,
                "updated_at": time.time(),
                "tail": {
                    "start_block": self._discovery_started_block,
                    "start_hash": self._discovery_started_hash,
                    "started_at": self._discovery_started_at,
                    "cursor": self._discovery_cursor or None,
                    "cursor_hash": self._discovery_cursor_hash or None,
                },
                "backfill": lanes,
            }
        payload["pools"] = [self._checkpoint_pool(pool) for pool in pools]
        try:
            with self._checkpoint_lock:
                self.index_checkpoint.parent.mkdir(parents=True, exist_ok=True)
                temporary = self.index_checkpoint.with_name(
                    f".{self.index_checkpoint.name}.{os.getpid()}.{id(self)}.tmp"
                )
                with temporary.open("w", encoding="utf-8") as handle:
                    json.dump(payload, handle, separators=(",", ":"), sort_keys=True)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(temporary, self.index_checkpoint)
            with self._lock:
                self._checkpoint_last_write = now_mono
                self._checkpoint_written_at = time.time()
                self._checkpoint_error = None
                if self._checkpoint_revision == revision:
                    self._checkpoint_dirty = False
        except Exception as exc:
            with self._lock:
                self._checkpoint_error = f"index checkpoint write: {exc}"



    @staticmethod
    def _decode_creation(log: dict[str, Any]) -> _Pool | None:
        emitter = str(log.get("address") or "").lower()
        topics = [str(topic).lower() for topic in (log.get("topics") or [])]
        if emitter not in CREATION_EMITTERS or not topics:
            return None
        topic0 = topics[0]
        words = _words(log.get("data"))
        block = _hex_int(log.get("blockNumber"))

        if emitter == POOL_MANAGER and topic0 == V4_INITIALIZE_TOPIC:
            if len(topics) < 4 or len(words) < 5:
                raise MarketError("malformed V4 Initialize event")
            pool_id = topics[1]
            token0, token1 = _topic_address(topics[2]), _topic_address(topics[3])
            configured_fee = int(words[0], 16) & 0xFFFFFF
            spacing = _signed_word(words[1], 24)
            hook = _word_address(words[2])
            dynamic = bool(configured_fee & 0x800000)
            pool = _Pool(
                pool_id,
                POOL_MANAGER,
                "v4",
                token0,
                token1,
                None if dynamic else configured_fee,
                spacing,
                hook,
                dynamic,
                POOL_MANAGER,
                block,
                "factory-live",
            )
            if not _BYTES32_RE.fullmatch(pool_id) or _v4_pool_id(pool) != pool_id:
                raise MarketError("V4 Initialize PoolKey hash mismatch")
            return pool

        if emitter in V2_FACTORIES and topic0 == V2_PAIR_CREATED_TOPIC:
            if len(topics) < 3 or len(words) < 2:
                raise MarketError("malformed V2 PairCreated event")
            pool_id = _word_address(words[0])
            return _Pool(
                pool_id,
                pool_id,
                "v2",
                _topic_address(topics[1]),
                _topic_address(topics[2]),
                None,
                factory=emitter,
                created_block=block,
                source="factory-live",
            )

        if emitter in CONCENTRATED_FACTORIES and topic0 == SLIPSTREAM_POOL_CREATED_TOPIC:
            if len(topics) < 3:
                raise MarketError("malformed Slipstream PoolCreated event")
            if len(topics) >= 4 and len(words) >= 1:
                spacing = _signed_word(topics[3][2:].rjust(64, "0"), 24)
                pool_id = _word_address(words[-1])
            elif len(words) >= 2:
                spacing = _signed_word(words[0], 24)
                pool_id = _word_address(words[1])
            else:
                raise MarketError("malformed Slipstream PoolCreated event")
            return _Pool(
                pool_id,
                pool_id,
                "v3",
                _topic_address(topics[1]),
                _topic_address(topics[2]),
                None,
                spacing,
                factory=emitter,
                created_block=block,
                source="factory-live",
            )

        if emitter in CONCENTRATED_FACTORIES and topic0 == V3_POOL_CREATED_TOPIC:
            if len(topics) < 4 or len(words) < 2:
                raise MarketError("malformed V3 PoolCreated event")
            pool_id = _word_address(words[1])
            return _Pool(
                pool_id,
                pool_id,
                "v3",
                _topic_address(topics[1]),
                _topic_address(topics[2]),
                _hex_int(topics[3]) & 0xFFFFFF,
                _signed_word(words[0], 24),
                factory=emitter,
                created_block=block,
                source="factory-live",
            )

        if emitter in CONCENTRATED_FACTORIES and topic0 == V3_POOL_CREATED_COMPACT_TOPIC:
            if len(topics) < 3:
                raise MarketError("malformed compact V3 PoolCreated event")
            if len(topics) >= 4 and len(words) >= 1:
                fee = _hex_int(topics[3]) & 0xFFFFFF
                pool_id = _word_address(words[-1])
            elif len(words) >= 2:
                fee = int(words[0], 16) & 0xFFFFFF
                pool_id = _word_address(words[1])
            else:
                raise MarketError("malformed compact V3 PoolCreated event")
            return _Pool(
                pool_id,
                pool_id,
                "v3",
                _topic_address(topics[1]),
                _topic_address(topics[2]),
                fee,
                factory=emitter,
                created_block=block,
                source="factory-live",
            )

        return None

    def _restart_discovery_after_reorg(self, latest: dict[str, Any], reason: str) -> None:
        latest_number = _hex_int(latest.get("number"))
        with self._lock:
            start = self._discovery_started_block
        anchor_number = latest_number if start is None or latest_number < start else start
        anchor = self.rpc.call("eth_getBlockByNumber", [hex(anchor_number), False])
        if not isinstance(anchor, dict) or not anchor.get("hash"):
            raise RpcError("reorg discovery anchor is unavailable")
        message = f"factory tail reorg at block {self._discovery_cursor}: {reason}; replaying"
        with self._lock:
            self._discovered = {
                pool_id: pool
                for pool_id, pool in self._discovered.items()
                if pool.source != "factory-live"
            }
            self._discovery_cursor = anchor_number
            self._discovery_cursor_hash = str(anchor["hash"]).lower()
            if self._discovery_started_block == anchor_number:
                self._discovery_started_hash = self._discovery_cursor_hash
            if start is None or latest_number < start:
                self._discovery_started_block = anchor_number
                self._discovery_started_hash = self._discovery_cursor_hash
                self._discovery_started_at = time.time()
            self._discovery_reorgs += 1
            self._discovery_last_reorg = message
            self._discovery_error = message
            self._catalog_cache.clear()
            self._mark_checkpoint_dirty_locked()

    def _refresh_discovery(self, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            if not force and now - self._discovery_last_poll < DISCOVERY_REFRESH_S:
                return
            self._discovery_last_poll = now
            header = dict(self._head) if self._head else None
            cursor = self._discovery_cursor
            cursor_hash = self._discovery_cursor_hash
        if not header:
            return
        latest_number = _hex_int(header.get("number"))
        latest_hash = str(header.get("hash") or "").lower()
        if not latest_hash:
            return

        try:
            if cursor == 0:
                canonical = self.rpc.call("eth_getBlockByNumber", [hex(latest_number), False])
                if not isinstance(canonical, dict) or str(canonical.get("hash") or "").lower() != latest_hash:
                    raise ReorgDetected("startup header changed before factory tail anchor")
                with self._lock:
                    self._discovery_started_block = latest_number
                    self._discovery_started_hash = latest_hash
                    self._discovery_started_at = time.time()
                    self._discovery_cursor = latest_number
                    self._discovery_cursor_hash = latest_hash
                    self._discovery_error = None
                    self._catalog_cache.clear()
                    self._mark_checkpoint_dirty_locked()
                return

            canonical_cursor = self.rpc.call("eth_getBlockByNumber", [hex(cursor), False])
            canonical_cursor_hash = (
                str(canonical_cursor.get("hash") or "").lower()
                if isinstance(canonical_cursor, dict)
                else ""
            )
            if latest_number < cursor or canonical_cursor_hash != cursor_hash:
                self._restart_discovery_after_reorg(
                    header,
                    "head moved backwards" if latest_number < cursor else "cursor hash changed",
                )
                return
            if latest_number == cursor:
                with self._lock:
                    self._discovery_error = None
                return

            target = min(latest_number, cursor + MAX_CATCHUP_BLOCKS)
            target_header = (
                header
                if target == latest_number
                else self.rpc.call("eth_getBlockByNumber", [hex(target), False])
            )
            if not isinstance(target_header, dict) or not target_header.get("hash"):
                raise RpcError(f"factory tail target block {target} is unavailable")
            target_hash = str(target_header["hash"]).lower()
            logs = self.rpc.call("eth_getLogs", [{
                "fromBlock": hex(cursor + 1),
                "toBlock": hex(target),
                "address": sorted(CREATION_EMITTERS),
                "topics": [list(CREATION_TOPICS)],
            }])
            if not isinstance(logs, list):
                raise RpcError("factory creation log response is malformed")
            decoded: list[_Pool] = []
            for log in logs:
                if not isinstance(log, dict):
                    raise MarketError("factory creation log is malformed")
                pool = self._decode_creation(log)
                if pool is None:
                    raise MarketError("unrecognized creation event from known factory")
                decoded.append(pool)
            confirmed = self.rpc.call("eth_getBlockByNumber", [hex(target), False])
            if not isinstance(confirmed, dict) or str(confirmed.get("hash") or "").lower() != target_hash:
                raise ReorgDetected(f"factory tail target block {target} changed during refresh")

            with self._lock:
                additions: dict[str, _Pool] = {}
                for pool in decoded:
                    existing = self._discovered.get(pool.id) or self.universe.by_id.get(pool.id)
                    if existing is not None:
                        if not self._pool_metadata_agrees(existing, pool):
                            raise MarketError(f"creation event conflicts with census for {pool.id}")
                        continue
                    additions[pool.id] = pool
                self._discovered.update(additions)
                self._discovery_cursor = target
                self._discovery_cursor_hash = target_hash
                self._discovery_error = None
                self._mark_checkpoint_dirty_locked()
                if additions:
                    self._catalog_cache.clear()
        except ReorgDetected as exc:
            try:
                self._restart_discovery_after_reorg(header, str(exc))
            except Exception as restart_exc:
                with self._lock:
                    self._discovery_error = f"factory tail reorg recovery: {restart_exc}"
        except Exception as exc:
            with self._lock:
                self._discovery_error = f"factory tail: {exc}"
    @staticmethod
    def _make_backfill_lane(floor: int, high_block: int, high_hash: str) -> dict[str, Any]:
        return {
            "floor": floor,
            "next_to": high_block,
            "high_block": high_block,
            "high_hash": high_hash,
            "low_block": None,
            "low_hash": None,
            "complete": floor > high_block,
            "chunk": BACKFILL_INITIAL_CHUNK,
            "blocks_scanned": 0,
            "logs_scanned": 0,
            "new_pools": 0,
            "failures": 0,
            "consecutive_failures": 0,
            "error": None,
            "updated_at": time.time(),
        }

    def _ensure_backfill_lanes(self) -> bool:
        with self._lock:
            start = self._discovery_started_block
            start_hash = self._discovery_started_hash
            if start is None or not start_hash:
                return False
            v4_head = _v4_census_head()
            v4_floor = (v4_head + 1) if v4_head is not None else 0
            needs_reset = bool(self._backfill_lanes) and any(
                int(lane.get("high_block") or -1) != start
                or str(lane.get("high_hash") or "").lower() != start_hash
                for lane in self._backfill_lanes.values()
            )
            if needs_reset:
                self._discovered = {
                    pool_id: pool
                    for pool_id, pool in self._discovered.items()
                    if not pool.source.startswith("backfill-")
                }
                self._backfill_lanes.clear()
                self._backfill_error = "backfill anchor changed after a reorg; restarting verified scans"
            if not self._backfill_lanes:
                self._backfill_lanes = {
                    "v4": self._make_backfill_lane(v4_floor, start, start_hash),
                    "legacy": self._make_backfill_lane(0, start, start_hash),
                }
                self._mark_checkpoint_dirty_locked()
                self._catalog_cache.clear()
                return True
            lane = self._backfill_lanes.get("v4")
            if lane is None:
                self._backfill_lanes["v4"] = self._make_backfill_lane(v4_floor, start, start_hash)
                self._mark_checkpoint_dirty_locked()
            elif int(lane["floor"]) != v4_floor:
                old_floor = int(lane["floor"])
                lane["floor"] = v4_floor
                if v4_floor > start:
                    lane["next_to"] = start
                elif v4_floor < old_floor and int(lane["next_to"]) < v4_floor:
                    lane["next_to"] = old_floor - 1
                if lane.get("low_block") is not None and int(lane["low_block"]) < v4_floor:
                    lane["low_block"] = None
                    lane["low_hash"] = None
                lane["complete"] = v4_floor > start or int(lane["next_to"]) < v4_floor
                lane["updated_at"] = time.time()
                self._mark_checkpoint_dirty_locked()
            if "legacy" not in self._backfill_lanes:
                self._backfill_lanes["legacy"] = self._make_backfill_lane(0, start, start_hash)
                self._mark_checkpoint_dirty_locked()
            return True

    def _backfill_header(self, number: int) -> dict[str, Any]:
        header = self._backfill_rpc.call("eth_getBlockByNumber", [hex(number), False])
        if not isinstance(header, dict) or not header.get("hash"):
            raise RpcError(f"backfill block {number} header is unavailable")
        return header

    @staticmethod
    def _backfill_pool(pool: _Pool, lane_name: str) -> _Pool:
        return _Pool(
            pool.id,
            pool.address,
            pool.kind,
            pool.token0,
            pool.token1,
            pool.fee_ppm,
            pool.tick_spacing,
            pool.hook,
            pool.dynamic_fee,
            pool.factory,
            pool.created_block,
            f"backfill-{lane_name}",
        )

    def _scan_backfill_lane(self, lane_name: str) -> bool:
        with self._lock:
            lane = dict(self._backfill_lanes[lane_name])
            has_active_selection = bool(self._selected)
        if lane["complete"]:
            return False
        floor = int(lane["floor"])
        to_block = int(lane["next_to"])
        chunk = min(
            int(lane["chunk"]),
            BACKFILL_ACTIVE_SELECTION_CHUNK if has_active_selection else BACKFILL_MAX_CHUNK,
        )
        from_block = max(floor, to_block - chunk + 1)
        high_header = self._backfill_header(int(lane["high_block"]))
        if str(high_header["hash"]).lower() != str(lane["high_hash"]).lower():
            raise ReorgDetected(f"{lane_name} backfill high anchor changed")
        if lane.get("low_block") is not None:
            low_header = self._backfill_header(int(lane["low_block"]))
            if str(low_header["hash"]).lower() != str(lane.get("low_hash") or "").lower():
                raise ReorgDetected(f"{lane_name} backfill low frontier changed")
        prior_to = self._backfill_header(to_block)

        if lane_name == "v4":
            addresses = [POOL_MANAGER]
            topics = [V4_INITIALIZE_TOPIC]
        else:
            addresses = sorted(V2_FACTORIES | CONCENTRATED_FACTORIES)
            topics = [
                V3_POOL_CREATED_TOPIC,
                V3_POOL_CREATED_COMPACT_TOPIC,
                SLIPSTREAM_POOL_CREATED_TOPIC,
                V2_PAIR_CREATED_TOPIC,
            ]
        logs = self._backfill_rpc.call("eth_getLogs", [{
            "fromBlock": hex(from_block),
            "toBlock": hex(to_block),
            "address": addresses,
            "topics": [topics],
        }])
        if not isinstance(logs, list):
            raise RpcError(f"{lane_name} backfill log response is malformed")
        confirmed_to, confirmed_from = self._backfill_rpc.batch([
            ("eth_getBlockByNumber", [hex(to_block), False]),
            ("eth_getBlockByNumber", [hex(from_block), False]),
        ])
        if (
            not isinstance(confirmed_to, dict)
            or str(confirmed_to.get("hash") or "").lower()
            != str(prior_to.get("hash") or "").lower()
        ):
            raise ReorgDetected(f"{lane_name} backfill block {to_block} changed during scan")
        if not isinstance(confirmed_from, dict) or not confirmed_from.get("hash"):
            raise RpcError(f"{lane_name} backfill lower frontier header is unavailable")

        decoded: list[_Pool] = []
        for log in logs:
            if not isinstance(log, dict):
                raise MarketError(f"{lane_name} backfill returned a malformed log")
            if log.get("removed") is True:
                raise ReorgDetected(f"{lane_name} backfill returned a removed log")
            block = _hex_int(log.get("blockNumber"))
            if block < from_block or block > to_block:
                raise MarketError(f"{lane_name} backfill log is outside its requested range")
            pool = self._decode_creation(log)
            if pool is None:
                raise MarketError(f"{lane_name} backfill saw an unsupported creation event")
            if (lane_name == "v4") != (pool.kind == "v4"):
                raise MarketError(f"{lane_name} backfill event family mismatch")
            decoded.append(self._backfill_pool(pool, lane_name))

        with self._lock:
            current = self._backfill_lanes[lane_name]
            if int(current["next_to"]) != to_block:
                raise MarketError(f"{lane_name} backfill frontier changed concurrently")
            additions: dict[str, _Pool] = {}
            for pool in decoded:
                existing = self._discovered.get(pool.id) or self.universe.by_id.get(pool.id)
                if existing is not None:
                    if not self._pool_metadata_agrees(existing, pool):
                        raise MarketError(f"{lane_name} backfill conflicts with {pool.id}")
                    continue
                prior = additions.get(pool.id)
                if prior is not None and not self._pool_metadata_agrees(prior, pool):
                    raise MarketError(f"{lane_name} backfill duplicates conflict for {pool.id}")
                additions[pool.id] = pool
            self._discovered.update(additions)
            current["next_to"] = from_block - 1
            current["low_block"] = from_block
            current["low_hash"] = str(confirmed_from["hash"]).lower()
            current["complete"] = int(current["next_to"]) < int(current["floor"])
            current["chunk"] = min(
                BACKFILL_MAX_CHUNK,
                max(BACKFILL_MIN_CHUNK, int(current["chunk"]) * 5 // 4),
            )
            current["blocks_scanned"] = int(current["blocks_scanned"]) + to_block - from_block + 1
            current["logs_scanned"] = int(current["logs_scanned"]) + len(logs)
            current["new_pools"] = int(current["new_pools"]) + len(additions)
            current["consecutive_failures"] = 0
            current["error"] = None
            current["updated_at"] = time.time()
            self._backfill_error = None
            self._mark_checkpoint_dirty_locked()
            if additions:
                self._catalog_cache.clear()
        return True

    def _backfill_step(self) -> bool:
        now = time.monotonic()
        with self._lock:
            if now < self._backfill_retry_after:
                return False
        try:
            if not self._backfill_chain_verified:
                chain_id = _hex_int(self._backfill_rpc.call("eth_chainId", []))
                if chain_id != CHAIN_ID:
                    raise RpcError(f"backfill RPC wrong chain id {chain_id}; expected {CHAIN_ID}")
                self._backfill_chain_verified = True
            if not self._ensure_backfill_lanes():
                return False
            with self._lock:
                lane_name = next(
                    (
                        name
                        for name in ("v4", "legacy")
                        if not bool(self._backfill_lanes[name]["complete"])
                    ),
                    None,
                )
            if lane_name is None:
                return False
            return self._scan_backfill_lane(lane_name)
        except Exception as exc:
            with self._lock:
                lane_name = next(
                    (
                        name
                        for name in ("v4", "legacy")
                        if name in self._backfill_lanes
                        and not bool(self._backfill_lanes[name]["complete"])
                    ),
                    None,
                )
                message = f"historical pool indexing: {exc}"
                if lane_name is not None:
                    lane = self._backfill_lanes[lane_name]
                    lane["failures"] = int(lane["failures"]) + 1
                    lane["consecutive_failures"] = int(lane["consecutive_failures"]) + 1
                    lane["chunk"] = max(BACKFILL_MIN_CHUNK, int(lane["chunk"]) // 2)
                    lane["error"] = message
                    lane["updated_at"] = time.time()
                    delay = min(10.0, 0.5 * (2 ** min(5, int(lane["consecutive_failures"]) - 1)))
                else:
                    delay = 1.0
                self._backfill_error = message
                self._backfill_retry_after = time.monotonic() + delay
                self._mark_checkpoint_dirty_locked()
            return False

    def _backfill_run(self) -> None:
        try:
            while not self._stop.is_set():
                progressed = self._backfill_step()
                self._write_index_checkpoint()
                self._stop.wait(BACKFILL_PAUSE_S if progressed else 0.5)
        finally:
            self._write_index_checkpoint(force=True)


    # ------------------------------------------------------------- live caches
    def _refresh_head(self, force: bool = False) -> None:
        now = time.monotonic()
        with self._lock:
            if not force and now - self._head_read_at < HEAD_REFRESH_S:
                return
        try:
            chain_raw, header = self.rpc.batch([
                ("eth_chainId", []),
                ("eth_getBlockByNumber", ["latest", False]),
            ])
            chain_id = _hex_int(chain_raw)
            if chain_id != CHAIN_ID:
                raise RpcError(f"wrong chain id {chain_id}; expected {CHAIN_ID}")
            if not isinstance(header, dict) or not header.get("hash") or header.get("number") is None:
                raise RpcError("latest block header is malformed")
            with self._lock:
                self._head = header
                self._head_error = None
                self._head_read_at = time.monotonic()
                self._chain_verified = True
        except Exception as exc:
            with self._lock:
                self._head_error = f"RPC: {exc}"
                self._head_read_at = time.monotonic()

    def _next_selection(self, now: float | None = None) -> _Selection | None:
        """Publish new selections first, then refresh active views oldest-first.

        Stream waiters keep selections active without copying their snapshots.
        Activity must not let one view starve another client's first load or
        refresh. Hidden views retain their cache without spending live RPC.
        """
        observed = time.monotonic() if now is None else now
        with self._lock:
            expired = [
                key
                for key, selection in self._selected.items()
                if observed - selection.last_used > SELECTION_TTL_S
            ]
            for key in expired:
                self._selected.pop(key, None)
            candidate = None
            for selection in reversed(self._selected.values()):
                active = observed - selection.last_used <= SELECTION_ACTIVE_S
                first_attempt = selection.snapshot is None and selection.last_refresh == 0.0
                if (
                    not (active or first_attempt)
                    or observed - selection.last_refresh < SELECT_REFRESH_S
                ):
                    continue
                if first_attempt:
                    return selection
                if candidate is None or selection.last_refresh < candidate.last_refresh:
                    candidate = selection
            return candidate

    def _run(self) -> None:
        while not self._stop.is_set():
            self._selection_wake.clear()
            self._refresh_head()
            selection = self._next_selection()
            if selection is not None:
                self._poll_selection(selection)
                continue
            if not self._external_index:
                self._refresh_discovery()
            selection = self._next_selection()
            if selection is not None:
                self._poll_selection(selection)
                continue
            self._selection_wake.wait(0.1)

    def _maintenance_run(self) -> None:
        """Keep historical discovery and catalog metadata off the live RPC lane."""
        while not self._stop.is_set():
            with self._lock:
                now = time.monotonic()
                selected = [
                    selection
                    for selection in reversed(tuple(self._selected.values()))
                    if now - selection.last_used <= SELECTION_ACTIVE_S
                ]
                metadata_page = [
                    self._page_metadata.popitem(last=False)[1]
                    for _ in range(min(20, len(self._page_metadata)))
                ]
                header = dict(self._head) if self._head else None
            worked = False
            for selection in selected:
                if self._stop.is_set():
                    break
                phase = "recent_history"
                try:
                    if self._load_recent_history(selection):
                        worked = True
                        break
                    phase = "participant_refresh"
                    if self._refresh_selected_participants(selection):
                        worked = True
                        break
                    phase = "position_history"
                    if self._scan_older_positions(selection):
                        worked = True
                        break
                except Exception as exc:
                    with selection.lock:
                        message = (
                            f"off-lane {phase.replace('_', ' ')} failed: {exc}"
                        )
                        selection.snapshot_dirty = True
                        self._selection_wake.set()
                        if phase == "recent_history":
                            selection.history_error = message
                        else:
                            selection.participant_refresh_error = message
                        if isinstance(exc, ReorgDetected):
                            selection.reorgs += 1
                            selection.last_reorg = str(exc)
                            selection.needs_seed = True
                            self._invalidate_participant_accounting(selection, message)
                            self._mark_selection_error(selection, "reorg", message)
            if metadata_page and header and not self._stop.is_set():
                self._resolve_page_metadata(
                    metadata_page,
                    hex(_hex_int(header["number"])),
                    rpc_client=self._maintenance_rpc,
                )
                resolved_ids = {pool.id for pool in metadata_page}
                with self._lock:
                    dirty_candidates = [
                        candidate
                        for candidate in self._selected.values()
                        if candidate.pool.id in resolved_ids
                    ]
                for candidate in dirty_candidates:
                    with candidate.lock:
                        candidate.snapshot_dirty = True
                self._selection_wake.set()
                worked = True
            self._stop.wait(0.05 if worked else 0.25)

    # ---------------------------------------------------------------- catalog
    def _token_object(self, address: str) -> dict[str, Any]:
        metadata = self.universe.tokens.get(address)
        symbol = metadata.symbol if metadata else None
        decimals = metadata.decimals if metadata else None
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
            "metadata_source": metadata.source if metadata else None,
        }

    def _protocol(self, pool: _Pool) -> str:
        if pool.factory == SLIPSTREAM_FACTORY:
            return "slipstream"
        if pool.factory in V3_FACTORIES and pool.factory != UNISWAP_V3_FACTORY:
            return "concentrated-liquidity"
        return {"v2": "v2-compatible", "v3": "v3-compatible", "v4": "uniswap-v4"}[pool.kind]

    def _catalog_row(self, pool: _Pool, board: dict[str, dict[str, Any]], now: float) -> dict[str, Any]:
        overlay = board.get(pool.id)
        token0 = self._token_object(pool.token0)
        token1 = self._token_object(pool.token1)
        last_swap = overlay.get("_last_swap_at") if overlay else None
        board_state = str(self._board_health.get("state") or "")
        if overlay and board_state == "live":
            state = "live"
        elif overlay:
            state = "stale"
        else:
            # No recent on-chain sample is not evidence of inactivity.
            state = "unobserved"
        fee = pool.fee_ppm
        if overlay and overlay.get("fee") is not None:
            fee = int(overlay["fee"])
        return {
            "id": pool.id,
            "address": pool.address,
            "kind": pool.kind,
            "protocol": self._protocol(pool),
            "pair": (
                f"{token0['symbol'] or token0['address']}/"
                f"{token1['symbol'] or token1['address']}"
            ),
            "token0": token0,
            "token1": token1,
            "fee_ppm": fee,
            "tick_spacing": pool.tick_spacing,
            # This public surface does not infer TVL from deployable capacity.
            "tvl_usd": None,
            # Exact one-hour USD notional and fees require observed swaps.
            "volume_1h_usd": None,
            "fees_1h_usd": None,
            "swaps_1h": int(overlay.get("swaps") or 0) if overlay else None,
            "last_swap_at": float(last_swap) if last_swap is not None else None,
            "block": int(overlay.get("block") or 0) if overlay else 0,
            "state": state,
            "provenance": {
                "source": pool.source,
                "factory": pool.factory,
                "created_block": pool.created_block,
            },
        }

    def _matches(self, pool: _Pool, query: str, kind: str | None) -> bool:
        if kind and pool.kind != kind:
            return False
        if not query:
            return True
        metadata0 = self.universe.tokens.get(pool.token0)
        metadata1 = self.universe.tokens.get(pool.token1)
        haystack = (
            pool.id, pool.address, pool.token0, pool.token1,
            metadata0.symbol.lower() if metadata0 and metadata0.symbol else "",
            metadata1.symbol.lower() if metadata1 and metadata1.symbol else "",
            pool.kind, self._protocol(pool),
        )
        return all(any(term in value for value in haystack) for term in re.split(r"[\s/]+", query) if term)

    def _resolve_page_metadata(
        self,
        pools: list[_Pool],
        block_tag: str,
        rpc_client: _Rpc | None = None,
    ) -> None:
        client = rpc_client or self.rpc
        now = time.monotonic()
        metadata_changed = False
        unresolved: list[str] = []
        seen: set[str] = set()
        for pool in pools:
            for address in (pool.token0, pool.token1):
                if address == NATIVE or address in seen:
                    continue
                metadata = self.universe.tokens.setdefault(address, _Token())
                if metadata.symbol and metadata.decimals is not None:
                    continue
                if self._token_retry_after.get(address, 0.0) > now:
                    continue
                seen.add(address)
                unresolved.append(address)
                if len(unresolved) >= MAX_TOKEN_RESOLVES_PER_PAGE:
                    break
            if len(unresolved) >= MAX_TOKEN_RESOLVES_PER_PAGE:
                break
        for start in range(0, len(unresolved), 40):
            addresses = unresolved[start : start + 40]
            calls: list[tuple[str, list[Any]]] = []
            for address in addresses:
                calls.extend((
                    ("eth_call", [{"to": address, "data": SYMBOL_SELECTOR}, block_tag]),
                    ("eth_call", [{"to": address, "data": DECIMALS_SELECTOR}, block_tag]),
                ))
            try:
                results = client.batch(calls)
            except Exception as exc:
                if client is self.rpc:
                    with self._lock:
                        self._head_error = f"RPC token metadata: {exc}"
                for address in addresses:
                    self._token_retry_after[address] = now + 30.0
                continue
            for index, address in enumerate(addresses):
                symbol_raw, decimals_raw = results[index * 2 : index * 2 + 2]
                metadata = self.universe.tokens.setdefault(address, _Token())
                symbol = _decode_symbol(symbol_raw)
                try:
                    decimals = _hex_int(decimals_raw)
                    if not 0 <= decimals <= 255:
                        decimals = None
                except (TypeError, ValueError):
                    decimals = None
                if symbol and metadata.symbol != symbol:
                    metadata.symbol = symbol
                    metadata_changed = True
                if decimals is not None and metadata.decimals != decimals:
                    metadata.decimals = decimals
                    metadata_changed = True
                if symbol or decimals is not None:
                    if metadata.source != "block-pinned RPC":
                        metadata.source = "block-pinned RPC"
                        metadata_changed = True
                else:
                    self._token_retry_after[address] = now + 30.0
        if metadata_changed:
            with self._lock:
                self._index_publication_revision += 1
                self._catalog_cache.clear()

    def catalog(self, params: dict[str, Any]) -> dict[str, Any]:
        query = str(params.get("q") or "").strip().lower()[:128]
        kind_raw = str(params.get("kind") or "").strip().lower()
        kind = kind_raw or None
        if kind not in {None, "v2", "v3", "v4"}:
            raise ValueError("kind must be v2, v3, or v4")
        sort = str(params.get("sort") or "activity").strip().lower()
        if sort not in {"activity", "liquidity", "fees"}:
            raise ValueError("sort must be activity, liquidity, or fees")
        try:
            limit = min(100, max(1, int(params.get("limit") or 50)))
            offset = max(0, int(params.get("offset") or 0))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit and offset must be integers") from exc

        with self._lock:
            board = dict(self._board)
            board_generation = self._board_as_of
            header = dict(self._head) if self._head else None
            head_error = self._head_error
            universe = self.universe
            discovered = list(self._discovered.values())
            discovery_cursor = self._discovery_cursor
            discovery_error = self._discovery_error
            discovery_started_hash = self._discovery_started_hash
            discovery_cursor_hash = self._discovery_cursor_hash
            discovery_started_block = self._discovery_started_block
            discovery_started_at = self._discovery_started_at
            discovery_reorgs = self._discovery_reorgs
            discovery_last_reorg = self._discovery_last_reorg
            census_reload_error = self._census_reload_error
            census_checked_at = self._census_checked_at
            census_reloaded_at = self._census_reloaded_at
            census_generation = self._census_generation
            backfill_lanes = deepcopy(self._backfill_lanes)
            backfill_error = self._backfill_error
            checkpoint_error = self._checkpoint_error
            checkpoint_loaded_at = self._checkpoint_loaded_at
            checkpoint_written_at = self._checkpoint_written_at
            checkpoint_revision = self._checkpoint_revision
            board_health_key = (
                self._board_health.get("state"),
                self._board_health.get("error"),
            )
            cache_key = (
                query,
                kind,
                sort,
                limit,
                offset,
                board_generation,
                board_health_key,
                self._head_error,
                discovery_cursor,
                discovery_error,
                census_generation,
                checkpoint_revision,
            )
            cached = self._catalog_cache.get(cache_key)
            if cached is not None:
                self._catalog_cache.move_to_end(cache_key)
                return deepcopy(cached)

        discovered.sort(key=lambda pool: pool.id)
        live = [
            pool
            for pool in universe.pools
            if pool.id in board and self._matches(pool, query, kind)
        ]
        live.extend(
            pool
            for pool in discovered
            if pool.id in board and self._matches(pool, query, kind)
        )
        if sort == "activity":
            live.sort(key=lambda pool: (
                int(board[pool.id].get("swaps") or 0),
                _safe_number(board[pool.id].get("_last_swap_at")) or 0.0,
                pool.id,
            ), reverse=True)
        elif sort == "fees":
            live.sort(key=lambda pool: (
                _safe_number(board[pool.id].get("fees_day")) or -1.0, pool.id,
            ), reverse=True)
        else:
            # This is only an ordering signal; cap_usd remains explicitly absent
            # from tvl_usd because +/-1% capacity is not pool TVL.
            live.sort(key=lambda pool: (
                _safe_number(board[pool.id].get("cap_usd")) or -1.0, pool.id,
            ), reverse=True)

        ordered_live = {pool.id for pool in live}
        total = len(live)
        page: list[_Pool] = []
        position = 0
        for pool in live:
            if position >= offset and len(page) < limit:
                page.append(pool)
            position += 1
        for collection in (universe.pools, discovered):
            for pool in collection:
                if pool.id in ordered_live or not self._matches(pool, query, kind):
                    continue
                total += 1
                if position >= offset and len(page) < limit:
                    page.append(pool)
                position += 1

        with self._lock:
            for pool in page:
                self._page_metadata[pool.id] = pool
                self._page_metadata.move_to_end(pool.id)
            while len(self._page_metadata) > 100:
                self._page_metadata.popitem(last=False)
        now = time.time()
        rows = [self._catalog_row(pool, board, now) for pool in page]
        errors = list(universe.errors)
        counts = dict(universe.counts)
        for pool in discovered:
            counts[pool.kind] += 1
        sources = [dict(item) for item in universe.sources]
        historical_gap: list[dict[str, Any]] = []
        backfill_progress: dict[str, dict[str, Any]] = {}
        if backfill_lanes:
            for lane_name in ("v4", "legacy"):
                lane = backfill_lanes.get(lane_name)
                if lane is None:
                    continue
                floor = int(lane["floor"])
                high = int(lane["high_block"])
                next_to = int(lane["next_to"])
                total_blocks = max(0, high - floor + 1)
                remaining_blocks = (
                    0
                    if lane["complete"]
                    else max(0, next_to - floor + 1)
                )
                covered_blocks = max(0, total_blocks - remaining_blocks)
                backfill_progress[lane_name] = {
                    "state": (
                        "complete"
                        if lane["complete"]
                        else ("error" if lane.get("error") else "indexing")
                    ),
                    "scan_direction": "newest-to-oldest",
                    "floor": floor,
                    "next_to": None if lane["complete"] else next_to,
                    "covered_from": (
                        floor
                        if lane["complete"] and total_blocks
                        else (next_to + 1 if covered_blocks else None)
                    ),
                    "through_block": high,
                    "high_hash": lane.get("high_hash"),
                    "verified_low_block": lane.get("low_block"),
                    "verified_low_hash": lane.get("low_hash"),
                    "blocks_total": total_blocks,
                    "blocks_scanned": covered_blocks,
                    "progress_pct": (
                        100.0
                        if total_blocks == 0
                        else round(100.0 * covered_blocks / total_blocks, 4)
                    ),
                    "logs_scanned": int(lane["logs_scanned"]),
                    "new_pools": int(lane["new_pools"]),
                    "failures": int(lane["failures"]),
                    "chunk": int(lane["chunk"]),
                    "updated_at": lane.get("updated_at"),
                    "error": lane.get("error"),
                }
                if not lane["complete"]:
                    historical_gap.append({
                        "kind": "v4" if lane_name == "v4" else "v2,v3",
                        "state": "error" if lane.get("error") else "indexing",
                        "from_block": floor,
                        "to_block": next_to,
                        "reason": (
                            "verified creation-log backfill is scanning newest-to-oldest"
                        ),
                    })
        else:
            for source in sources:
                source_head = source.get("through_block")
                kind_name = str(source.get("kind") or "unknown")
                historical_gap.append({
                    "kind": kind_name,
                    "state": "unanchored",
                    "from_block": (
                        int(source_head) + 1 if source_head is not None else None
                    ),
                    "to_block": discovery_started_block,
                    "reason": "historical creation-log backfill has not anchored yet",
                })
        with self._lock:
            board_health = dict(self._board_health)
            if self._head_error:
                errors.append(self._head_error)
            head = dict(self._head) if self._head else None
            board_as_of = self._board_as_of
            token_known = sum(
                1
                for token in self.universe.tokens.values()
                if token.symbol or token.decimals is not None
            )
        head_number = _hex_int(head["number"]) if head else 0
        discovery_lag = max(0, head_number - discovery_cursor) if discovery_cursor else None
        if census_reload_error:
            errors.append(census_reload_error)
        if discovery_error:
            errors.append(discovery_error)
        if backfill_error:
            errors.append(backfill_error)
        if checkpoint_error:
            errors.append(checkpoint_error)
        if historical_gap:
            errors.append("pool universe historical continuity is incomplete; see coverage.historical_gap")
        if discovery_lag:
            errors.append(f"factory tail is {discovery_lag} blocks behind RPC head")
        state = "live" if not errors else ("error" if head is None else "degraded")
        result = {
            "chain_id": CHAIN_ID,
            "as_of": now,
            "block": head_number,
            "health": {"state": state, "error": "; ".join(errors) if errors else None},
            "total": total,
            "counts": counts,
            "rows": rows,
            "providers": self.source_status(),
            "coverage": {
                "universe": (
                    "known-factory-complete"
                    if not historical_gap and not discovery_lag and not backfill_error
                    else "indexing-known-factories"
                ),
                "historical_continuity": "complete" if not historical_gap else "indexing",
                "historical_gap": historical_gap,
                "sources": sources,
                "backfill": {
                    "state": (
                        "error"
                        if backfill_error
                        else ("complete" if not historical_gap and backfill_lanes else "indexing")
                    ),
                    "priority": "recent gaps first, then older V2/V3 history",
                    "lanes": backfill_progress,
                    "error": backfill_error,
                },
                "checkpoint": {
                    "path": _path_label(self.index_checkpoint),
                    "loaded_at": checkpoint_loaded_at,
                    "written_at": checkpoint_written_at,
                    "revision": checkpoint_revision,
                    "error": checkpoint_error,
                },
                "factory_tail": {
                    "emitters": len(CREATION_EMITTERS),
                    "verified_start_block": discovery_started_block,
                    "verified_start_hash": discovery_started_hash,
                    "started_at": discovery_started_at,
                    "through_block": discovery_cursor or None,
                    "through_hash": discovery_cursor_hash or None,
                    "head_lag_blocks": discovery_lag,
                    "state": (
                        "error"
                        if discovery_error
                        else ("catching_up" if discovery_lag else "live")
                    ),
                    "discovered_pools": len(discovered),
                    "reorgs": discovery_reorgs,
                    "last_reorg": discovery_last_reorg,
                    "error": discovery_error,
                },
                "census_reload": {
                    "generation": census_generation,
                    "checked_at": census_checked_at,
                    "reloaded_at": census_reloaded_at,
                    "error": census_reload_error,
                },
                "token_metadata": {
                    "known": token_known,
                    "policy": "public registry metadata plus block-pinned resolution for discovered pools",
                },
            },
        }
        with self._lock:
            self._catalog_cache[cache_key] = deepcopy(result)
            self._catalog_cache.move_to_end(cache_key)
            while len(self._catalog_cache) > 16:
                self._catalog_cache.popitem(last=False)
        return result

    # ---------------------------------------------------------- selected pools
    @staticmethod
    def _normalize_owner(owner: str | None) -> str | None:
        if owner is None or not str(owner).strip():
            return None
        normalized = str(owner).strip().lower()
        if not _ADDRESS_RE.fullmatch(normalized):
            raise ValueError("owner must be a 20-byte hex address")
        return normalized

    def _selection(self, pool_id: str, owner: str | None) -> _Selection:
        normalized_id = str(pool_id or "").strip().lower()
        pool = self._pool_by_id(normalized_id)
        if pool is None:
            raise ValueError(f"unknown pool id {pool_id!r}")
        normalized_owner = self._normalize_owner(owner)
        key = (pool.id, normalized_owner)
        with self._lock:
            selection = self._selected.get(key)
            if selection is None:
                selection = _Selection(pool, normalized_owner)
                self._selected[key] = selection
                while len(self._selected) > MAX_SELECTIONS:
                    self._selected.popitem(last=False)
            else:
                self._selected.move_to_end(key)
            metadata0 = self.universe.tokens.get(pool.token0)
            metadata1 = self.universe.tokens.get(pool.token1)
            if any(
                metadata is None
                or metadata.symbol is None
                or metadata.decimals is None
                for metadata in (metadata0, metadata1)
            ):
                self._page_metadata[pool.id] = pool
                self._page_metadata.move_to_end(pool.id)
                while len(self._page_metadata) > 100:
                    self._page_metadata.popitem(last=False)
            selection.last_used = time.monotonic()
        self._selection_wake.set()
        return selection

    def detail(self, pool_id: str, owner: str | None = None) -> dict[str, Any]:
        selection = self._selection(pool_id, owner)
        # Publication replaces the snapshot atomically; readers never wait for
        # RPC or a writer lock. One background worker owns live-chain refreshes.
        snapshot = selection.snapshot
        if snapshot is None:
            return self._error_detail(selection, "loading the first chain snapshot", "warming")
        return deepcopy(snapshot)

    def wait_detail(
            self, pool_id: str, owner: str | None, after_revision: int | None,
            timeout: float = 10.0,
    ) -> dict[str, Any] | None:
        """Wait for one immutable publication without polling or copying it."""
        selection = self._selection(pool_id, owner)
        key = (selection.pool.id, selection.owner)
        deadline = time.monotonic() + max(0.0, timeout)
        while not self._stop.is_set():
            with self._detail_cond:
                published = selection.snapshot
                if (
                    published is not None
                    and (
                        after_revision is None
                        or int(published.get("revision") or 0)
                        != int(after_revision)
                    )
                ):
                    return published
                now = time.monotonic()
                remaining = deadline - now
                if remaining <= 0:
                    return None
                current = self._selected.get(key)
                if current is selection:
                    selection.last_used = now
                    self._detail_cond.wait(
                        min(remaining, SELECTION_PREEMPT_S / 2),
                    )
                    continue
            # More than MAX_SELECTIONS distinct live views may evict this one.
            # Rejoin without rebuilding or copying a snapshot on every wake.
            selection = self._selection(pool_id, owner)
            key = (selection.pool.id, selection.owner)
        return None

    def _publish_detail(
            self, selection: _Selection, snapshot: dict[str, Any],
    ) -> None:
        with self._detail_cond:
            self._detail_revision += 1
            selection.snapshot_revision = self._detail_revision
            snapshot["revision"] = self._detail_revision
            selection.snapshot = snapshot
            self._detail_cond.notify_all()

    def _selection_superseded(self, selection: _Selection) -> bool:
        """Cooperatively stop costly hydration after a visible pool change."""
        now = time.monotonic()
        with self._lock:
            tracked = any(candidate is selection for candidate in self._selected.values())
            if not tracked:
                return True
            if now - selection.last_used <= SELECTION_PREEMPT_S:
                return False
            return any(
                candidate is not selection
                and candidate.last_used > selection.last_used
                and now - candidate.last_used <= SELECTION_ACTIVE_S
                for candidate in self._selected.values()
            )

    def _poll_selection(self, selection: _Selection) -> None:
        if not selection.lock.acquire(blocking=False):
            self._stop.wait(0.01)
            return
        try:
            if self._stop.is_set() or self._next_selection() is not selection:
                return
            try:
                updated = (
                    self._seed_selection(selection)
                    if selection.needs_seed
                    else self._advance_selection(selection)
                )
                if updated and selection.disconnected:
                    selection.reconnects += 1
                    selection.disconnected = False
            except ReorgDetected as exc:
                selection.reorgs += 1
                selection.last_reorg = str(exc)
                selection.needs_seed = True
                self._invalidate_participant_accounting(selection, str(exc))
                self._mark_selection_error(selection, "reorg", str(exc))
            except Exception as exc:
                selection.refresh_failures += 1
                selection.disconnected = True
                # Fixed-state refreshes are idempotent. A transport interruption
                # must not discard the confirmed cursor or older-range scan.
                if not isinstance(exc, RpcError):
                    selection.needs_seed = True
                    self._invalidate_participant_accounting(selection, str(exc))
                self._mark_selection_error(selection, "error", f"RPC refresh failed: {exc}")
            finally:
                selection.last_refresh = time.monotonic()
        finally:
            selection.lock.release()

    def _head_header(self) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            if (
                self._chain_verified
                and self._head is not None
                and self._head_error is None
                and now - self._head_read_at <= HEAD_REFRESH_S
            ):
                return dict(self._head)
        chain_raw, header = self.rpc.batch([
            ("eth_chainId", []),
            ("eth_getBlockByNumber", ["latest", False]),
        ])
        chain_id = _hex_int(chain_raw)
        if chain_id != CHAIN_ID:
            raise RpcError(f"wrong chain id {chain_id}; expected {CHAIN_ID}")
        if not isinstance(header, dict) or not header.get("hash") or header.get("number") is None:
            raise RpcError("latest block header is malformed")
        with self._lock:
            self._head = dict(header)
            self._head_error = None
            self._head_read_at = time.monotonic()
            self._chain_verified = True
        return header
    def _confirm_header(self, header: dict[str, Any]) -> None:
        number = _hex_int(header["number"])
        expected_hash = str(header["hash"]).lower()
        canonical = self.rpc.call("eth_getBlockByNumber", [hex(number), False])
        if (
            not isinstance(canonical, dict)
            or str(canonical.get("hash") or "").lower() != expected_hash
        ):
            raise ReorgDetected(
                f"block {number} changed while its selected-pool snapshot was being read"
            )

    def _pinned_state_batch(
        self,
        calls: Iterable[tuple[str, list[Any]]],
        block_tag: str,
    ) -> list[Any]:
        """Collapse exact-block contract reads without weakening any sub-read.

        JSON-RPC batching still makes the remote node execute a large selected
        curve as hundreds of separate top-level calls, and the 100-call client
        bound turns dense pools into many serialized HTTP round trips.
        Multicall3 is already verified on this chain; its per-call success bits
        let us reject any unavailable value instead of decoding it as zero.
        A provider which cannot execute Multicall3 falls back to the existing
        bounded JSON-RPC path.
        """
        specifications = list(calls)
        if not specifications:
            return []
        packed: list[tuple[str, str]] = []
        for method, params in specifications:
            if (
                method != "eth_call"
                or len(params) != 2
                or params[1] != block_tag
                or not isinstance(params[0], dict)
            ):
                raise ValueError("pinned state batch requires exact-block eth_call entries")
            target = str(params[0].get("to") or "")
            data = str(params[0].get("data") or "")
            if not _ADDRESS_RE.fullmatch(target.lower()) or not data.startswith("0x"):
                raise ValueError("pinned state batch contains malformed call data")
            packed.append((target, data))
        if len(specifications) == 1:
            method, params = specifications[0]
            return [self.rpc.call(method, params)]

        output: list[Any] = []
        for offset in range(0, len(packed), MAX_STATE_MULTICALL_CALLS):
            chunk = packed[offset : offset + MAX_STATE_MULTICALL_CALLS]
            chunk_specs = specifications[offset : offset + MAX_STATE_MULTICALL_CALLS]
            try:
                raw = self.rpc.call("eth_call", [{
                    "to": _mc.MULTICALL3,
                    "data": _mc.encode(chunk),
                }, block_tag])
                if not isinstance(raw, str) or not raw.startswith("0x"):
                    raise ValueError("Multicall3 returned non-hex data")
                decoded = list(_mc.decode(raw))
                if len(decoded) != len(chunk):
                    raise ValueError("Multicall3 response arity mismatch")
            except Exception:
                output.extend(self._rpc_batches(chunk_specs))
                continue
            failed = [offset + index for index, (ok, _value) in enumerate(decoded) if not ok]
            if failed:
                raise RpcError(
                    "block-pinned Multicall3 sub-call failed at "
                    + ", ".join(str(index) for index in failed[:8])
                )
            output.extend("0x" + bytes(value).hex() for _ok, value in decoded)
        return output

    def _read_core(self, selection: _Selection, block_tag: str, seed: bool) -> dict[str, Any]:
        pool = selection.pool
        if pool.kind == "v3":
            calls = [
                ("eth_call", [{"to": pool.address, "data": SLOT0_SELECTOR}, block_tag]),
                ("eth_call", [{"to": pool.address, "data": LIQUIDITY_SELECTOR}, block_tag]),
            ]
            proof_fee = pool.fee_ppm if seed else None
            if seed:
                calls.extend((
                    ("eth_call", [{"to": pool.address, "data": FEE_SELECTOR}, block_tag]),
                    ("eth_call", [{"to": pool.address, "data": TICK_SPACING_SELECTOR}, block_tag]),
                    ("eth_call", [{"to": pool.address, "data": FACTORY_SELECTOR}, block_tag]),
                    ("eth_call", [{"to": pool.address, "data": TOKEN0_SELECTOR}, block_tag]),
                    ("eth_call", [{"to": pool.address, "data": TOKEN1_SELECTOR}, block_tag]),
                ))
                if proof_fee is not None:
                    get_pool_data = (
                        GET_POOL_SELECTOR
                        + pool.token0[2:].rjust(64, "0")
                        + pool.token1[2:].rjust(64, "0")
                        + f"{proof_fee:064x}"
                    )
                    calls.append((
                        "eth_call",
                        [{"to": UNISWAP_V3_FACTORY, "data": get_pool_data}, block_tag],
                    ))
            results = self._pinned_state_batch(calls, block_tag)
            slot = _words(results[0])
            if len(slot) < 2:
                raise RpcError("V3 slot0 response is malformed")
            core = {
                "sqrt": int(slot[0], 16),
                "tick": _signed_word(slot[1]),
                "liquidity": _hex_int(results[1]),
            }
            if seed:
                selection.current_fee = _hex_int(results[2])
                spacing_words = _words(results[3])
                if not spacing_words:
                    raise RpcError("V3 tickSpacing response is malformed")
                spacing = _signed_word(spacing_words[0])
                if spacing <= 0:
                    raise RpcError(f"invalid V3 tick spacing {spacing}")
                selection.factory = _address_word(results[4])
                token0 = _address_word(results[5])
                token1 = _address_word(results[6])
                if (token0, token1) != (pool.token0, pool.token1):
                    raise RpcError(
                        f"V3 census token mismatch: RPC has {token0}/{token1}"
                    )
                selection.pool = _Pool(
                    pool.id,
                    pool.address,
                    pool.kind,
                    pool.token0,
                    pool.token1,
                    selection.current_fee,
                    spacing,
                    pool.hook,
                    pool.dynamic_fee,
                    selection.factory,
                    pool.created_block,
                    pool.source,
                )
                selection.factory_member = False
                selection.factory_membership_error = None
                if selection.factory == UNISWAP_V3_FACTORY:
                    try:
                        if proof_fee == selection.current_fee:
                            canonical_raw = results[7]
                        else:
                            get_pool_data = (
                                GET_POOL_SELECTOR
                                + pool.token0[2:].rjust(64, "0")
                                + pool.token1[2:].rjust(64, "0")
                                + f"{selection.current_fee:064x}"
                            )
                            canonical_raw = self.rpc.call(
                                "eth_call",
                                [{"to": UNISWAP_V3_FACTORY, "data": get_pool_data}, block_tag],
                            )
                        canonical_pool = _address_word(canonical_raw)
                        selection.factory_member = canonical_pool == pool.address
                        if not selection.factory_member:
                            selection.factory_membership_error = (
                                f"factory getPool returned {canonical_pool}"
                            )
                    except Exception as exc:
                        selection.factory_membership_error = (
                            f"factory getPool proof unavailable: {exc}"
                        )
            return core
        if pool.kind == "v4":
            if seed:
                pool = _resolve_v4_pool_key(pool)
                selection.pool = pool
            slot_raw, liquidity_raw = self._pinned_state_batch([
                ("eth_call", [{"to": STATE_VIEW, "data": SV_SLOT0_SELECTOR + pool.id[2:]}, block_tag]),
                ("eth_call", [{"to": STATE_VIEW, "data": SV_LIQUIDITY_SELECTOR + pool.id[2:]}, block_tag]),
            ], block_tag)
            slot = _words(slot_raw)
            liquidity = _words(liquidity_raw)
            if len(slot) < 4 or not liquidity:
                raise RpcError("V4 StateView response is malformed")
            selection.current_fee = int(slot[3], 16) & ((1 << 24) - 1)
            return {
                "sqrt": int(slot[0], 16),
                "tick": _signed_word(slot[1]),
                "liquidity": int(liquidity[0], 16),
            }
        calls: list[tuple[str, list[Any]]] = []
        if seed:
            calls.extend((
                ("eth_call", [{"to": pool.address, "data": TOKEN0_SELECTOR}, block_tag]),
                ("eth_call", [{"to": pool.address, "data": TOKEN1_SELECTOR}, block_tag]),
            ))
        reserve_index = len(calls)
        calls.append(("eth_call", [{"to": pool.address, "data": RESERVES_SELECTOR}, block_tag]))
        if selection.owner:
            calls.extend((
                ("eth_call", [{"to": pool.address, "data": TOTAL_SUPPLY_SELECTOR}, block_tag]),
                ("eth_call", [{"to": pool.address, "data": _balance_call(selection.owner)}, block_tag]),
            ))
        results = self._pinned_state_batch(calls, block_tag)
        if seed:
            token0, token1 = _address_word(results[0]), _address_word(results[1])
            if (token0, token1) != (pool.token0, pool.token1):
                raise RpcError(
                    f"V2 census token mismatch: RPC has {token0}/{token1}"
                )
        reserves = _words(results[reserve_index])
        if len(reserves) < 2:
            raise RpcError("V2 getReserves response is malformed")
        reserve0 = int(reserves[0], 16) & ((1 << 112) - 1)
        reserve1 = int(reserves[1], 16) & ((1 << 112) - 1)
        core = {"reserve0": reserve0, "reserve1": reserve1}
        if selection.owner:
            core["total_supply"] = _hex_int(results[reserve_index + 1])
            core["owner_balance"] = _hex_int(results[reserve_index + 2])
        return core


    def _logs(
        self, selection: _Selection, start: int, end: int,
        swaps_only: bool = False, rpc_client: _Rpc | None = None,
    ) -> list[dict[str, Any]]:
        if start > end:
            return []
        pool = selection.pool
        query: dict[str, Any] = {"fromBlock": hex(start), "toBlock": hex(end)}
        if pool.kind == "v3":
            query["address"] = pool.address
            query["topics"] = [V3_SWAP_TOPIC if swaps_only else [V3_SWAP_TOPIC, V3_MINT_TOPIC, V3_BURN_TOPIC, V3_COLLECT_TOPIC]]
        elif pool.kind == "v4":
            query["address"] = POOL_MANAGER
            query["topics"] = [[V4_SWAP_TOPIC] if swaps_only else [V4_SWAP_TOPIC, V4_MODIFY_TOPIC], pool.id]
        else:
            from .lp_market_protocols import V2_BURN_TOPIC, V2_MINT_TOPIC

            query["address"] = pool.address
            query["topics"] = [V2_SWAP_TOPIC if swaps_only else [V2_SWAP_TOPIC, V2_SYNC_TOPIC, V2_MINT_TOPIC, V2_BURN_TOPIC]]
        result: list[dict[str, Any]] = []
        for first in range(start, end + 1, SELECT_LOG_CHUNK):
            last = min(end, first + SELECT_LOG_CHUNK - 1)
            page = {**query, "fromBlock": hex(first), "toBlock": hex(last)}
            try:
                rows = (rpc_client or self.rpc).call("eth_getLogs", [page])
            except RpcError as exc:
                raise RpcError(f"selected pool logs [{first}, {last}]: {exc}") from exc
            if not isinstance(rows, list):
                raise RpcError("eth_getLogs returned a non-list")
            result.extend(rows)
        return sorted(result, key=lambda row: (
            _hex_int(row.get("blockNumber") or 0),
            _hex_int(row.get("transactionIndex") or 0),
            _hex_int(row.get("logIndex") or 0),
        ))

    def _participant_history_logs(
        self,
        selection: _Selection,
        start: int,
        end: int,
        rpc_client: _Rpc | None = None,
    ) -> list[dict[str, Any]]:
        if selection.pool.kind != "v3" or start > end:
            return []
        query = {
            "address": selection.pool.address,
            "fromBlock": hex(start),
            "toBlock": hex(end),
            "topics": [[V3_MINT_TOPIC, V3_BURN_TOPIC, V3_COLLECT_TOPIC]],
        }
        result = (rpc_client or self.rpc).call("eth_getLogs", [query])
        if not isinstance(result, list):
            raise RpcError("participant lifecycle scan returned a non-list")
        return sorted(result, key=lambda row: (
            _hex_int(row.get("blockNumber") or 0),
            _hex_int(row.get("transactionIndex") or 0),
            _hex_int(row.get("logIndex") or 0),
        ))

    def _remember_participants(
        self,
        selection: _Selection,
        logs: Iterable[dict[str, Any]],
        include: set[tuple[str, int, int]] | None = None,
    ) -> set[tuple[str, int, int]]:
        """Remember and return every accepted V3 storage owner/range touched."""
        touched: set[tuple[str, int, int]] = set()
        for log in logs:
            topics = [str(topic).lower() for topic in (log.get("topics") or [])]
            if len(topics) < 4 or topics[0] not in {
                V3_MINT_TOPIC,
                V3_BURN_TOPIC,
                V3_COLLECT_TOPIC,
            }:
                continue
            owner = _topic_address(topics[1])
            item = (owner, _signed_topic(topics[2]), _signed_topic(topics[3]))
            block = _hex_int(log.get("blockNumber") or 0)
            if include is not None and item not in include:
                continue
            touched.add(item)
            selection.participant_ranges[item] = max(
                block, selection.participant_ranges.get(item, 0)
            )
            if selection.owner == owner:
                own_range = (item[1], item[2])
                selection.ranges[own_range] = max(
                    block, selection.ranges.get(own_range, 0)
                )
        if len(selection.ranges) > MAX_POSITION_RANGES:
            selection.ranges_truncated = True
            newest = sorted(
                selection.ranges.items(), key=lambda item: item[1], reverse=True
            )[:MAX_POSITION_RANGES]
            selection.ranges = dict(newest)
        if len(selection.participant_ranges) > MAX_PARTICIPANT_RANGES:
            selection.participant_ranges_truncated = True
            newest_participants = sorted(
                selection.participant_ranges.items(),
                key=lambda item: (
                    item[0][0] == selection.owner,
                    int(selection.participant_state.get(item[0], {}).get("liquidity", 0)) > 0,
                    item[1],
                ),
                reverse=True,
            )[:MAX_PARTICIPANT_RANGES]
            selection.participant_ranges = dict(newest_participants)
            retained = set(selection.participant_ranges)
            selection.participant_state = {
                key: value
                for key, value in selection.participant_state.items()
                if key in retained
            }
            retained_ticks = {
                boundary
                for _owner, lower, upper in selection.participant_state
                for boundary in (lower, upper)
            }
            selection.participant_tick_fees = {
                boundary: value
                for boundary, value in selection.participant_tick_fees.items()
                if boundary in retained_ticks
            }
            selection.participant_accounting = {
                key: value
                for key, value in selection.participant_accounting.items()
                if key in retained
            }
            retained_owners = {
                owner for owner, _lower, _upper in selection.participant_ranges
            }
            selection.participant_kinds = {
                owner: kind
                for owner, kind in selection.participant_kinds.items()
                if owner in retained_owners
            }
            touched.intersection_update(retained)
        return touched

    def _load_recent_history(self, selection: _Selection) -> bool:
        """Enrich charts off-lane; never gate selected-pool spot/depth refresh."""
        if not selection.lock.acquire(blocking=False):
            return False
        try:
            now = time.monotonic()
            if (
                not selection.recent_history_pending or selection.needs_seed
                or selection.cursor <= 0
                or now - selection.last_recent_history_scan < PARTICIPANT_SCAN_INTERVAL_S
            ):
                return False
            selection.last_recent_history_scan = now
            generation = selection.history_generation
            block = selection.cursor
            block_hash = selection.cursor_hash
            snapshot = selection.snapshot or {}
            recent_cursor = selection.recent_history_cursor
            timestamp = int(snapshot.get("block_timestamp") or snapshot.get("as_of") or 0)
            header = {"number": hex(block), "hash": block_hash, "timestamp": hex(timestamp)}
        finally:
            selection.lock.release()
        if recent_cursor > 0:
            swap_start = recent_cursor + 1
            start_header = self._maintenance_rpc.call(
                "eth_getBlockByNumber", [hex(swap_start), False],
            )
            if not isinstance(start_header, dict) or start_header.get("timestamp") is None:
                raise RpcError(f"recent-history block {swap_start} is unavailable")
        else:
            swap_start, start_header = self._swap_seed_boundary(
                header, rpc_client=self._maintenance_rpc,
            )
        events = self._logs(
            selection, swap_start, block,
            rpc_client=self._maintenance_rpc,
        )
        timestamps = self._timestamps(events, rpc_client=self._maintenance_rpc)
        start_hash = str(start_header.get("hash") or "").lower()
        if not _BYTES32_RE.fullmatch(start_hash):
            raise RpcError(f"recent-history block {swap_start} hash is unavailable")
        start_anchor, end_anchor = self._maintenance_rpc.batch([
            ("eth_getBlockByNumber", [hex(swap_start), False]),
            ("eth_getBlockByNumber", [hex(block), False]),
        ])
        with selection.lock:
            if selection.needs_seed or selection.history_generation != generation:
                return False
        for number, anchor in ((swap_start, start_anchor), (block, end_anchor)):
            if not isinstance(anchor, dict) or not _BYTES32_RE.fullmatch(
                str(anchor.get("hash") or "").lower()
            ):
                raise RpcError(f"recent-history anchor block {number} is unavailable")
        if (
            not isinstance(start_anchor, dict)
            or str(start_anchor.get("hash") or "").lower() != start_hash
        ):
            raise ReorgDetected(
                f"recent-history start block {swap_start} changed"
            )
        if (
            not isinstance(end_anchor, dict)
            or str(end_anchor.get("hash") or "").lower() != block_hash
        ):
            raise ReorgDetected(
                f"recent-history anchor block {block} changed "
                f"from {block_hash} to {end_anchor['hash']}"
            )
        derived_events = self._derive_lp_events(selection, events, timestamps)
        with selection.lock:
            if (
                selection.needs_seed or selection.history_generation != generation
                or selection.cursor < block
            ):
                return False
            if (
                selection.swap_coverage_start_block is None
                or swap_start < selection.swap_coverage_start_block
            ):
                selection.swap_coverage_start_block = swap_start
                selection.swap_coverage_start_timestamp = _hex_int(start_header["timestamp"])
            selection.swap_coverage_through_block = max(
                block, selection.swap_coverage_through_block or -1,
            )
            if selection.swap_coverage_through_block == block:
                selection.swap_coverage_through_hash = block_hash
            self._append_swaps(selection, events, timestamps)
            self._append_owner_liquidity_changes(selection, events, timestamps)
            shape_blocks: list[int] = []
            for event in events:
                topics = event.get("topics") or ()
                if not topics:
                    continue
                topic = str(topics[0]).lower()
                words = _words(event.get("data"))
                changes_shape = (
                    topic == V3_MINT_TOPIC
                    or topic == V3_BURN_TOPIC
                    and bool(words) and int(words[0], 16) != 0
                    or topic == V4_MODIFY_TOPIC
                    and len(words) >= 3 and _signed_word(words[2]) != 0
                )
                if changes_shape:
                    shape_blocks.append(
                        _hex_int(event.get("blockNumber") or 0)
                    )
            state_block = selection.curve_state_block or -1
            unincorporated = [
                event_block
                for event_block in shape_blocks
                if event_block > state_block
            ]
            if unincorporated:
                dirty = min(unincorporated)
                selection.curve_dirty_from_block = min(
                    dirty, selection.curve_dirty_from_block or dirty,
                )
                selection.curve_verified_through_block = min(
                    selection.curve_verified_through_block or state_block,
                    dirty - 1,
                )
            elif (
                selection.curve_state_block is not None
                and swap_start
                <= (selection.curve_verified_through_block or state_block) + 1
            ):
                selection.curve_verified_through_block = max(
                    block, selection.curve_verified_through_block or state_block,
                )
            if selection.pool.kind == "v3":
                self._remember_participants(selection, events)
                self._apply_accounting_logs(selection, events)
                if selection.history_floor <= 0 or swap_start < selection.history_floor:
                    selection.history_floor = swap_start
                    selection.history_frontier_hash = str(start_header["hash"]).lower()
                selection.history_complete = selection.history_complete or swap_start == 0
                self._merge_lp_events(selection, derived_events)
            else:
                selection.history_floor = 0
                selection.history_frontier_hash = None
                selection.history_complete = True
            selection.recent_history_pending = selection.cursor > block
            selection.history_error = (
                "recent event history is catching up off the live state lane"
                if selection.recent_history_pending else None
            )
            selection.recent_history_cursor = max(
                selection.recent_history_cursor, block,
            )
            selection.snapshot_dirty = True
            self._selection_wake.set()
            return True


    def _scan_older_positions(self, selection: _Selection) -> bool:
        """Scan one verified all-participant lifecycle chunk off the live RPC."""
        if not selection.lock.acquire(blocking=False):
            return False
        try:
            now = time.monotonic()
            if (
                selection.pool.kind != "v3"
                or selection.history_complete
                or selection.needs_seed
                or selection.cursor <= 0
                or now - selection.last_history_scan < PARTICIPANT_SCAN_INTERVAL_S
            ):
                return False
            selection.last_history_scan = now
            if selection.history_floor <= 0:
                selection.history_complete = True
                return False
            frontier = selection.history_floor
            generation = selection.history_generation
            anchor_block = selection.cursor
            anchor_hash = selection.cursor_hash
            frontier_hash = selection.history_frontier_hash
        finally:
            selection.lock.release()

        if not self._maintenance_chain_verified:
            chain_id = _hex_int(self._maintenance_rpc.call("eth_chainId", []))
            if chain_id != CHAIN_ID:
                raise RpcError(
                    f"participant-history RPC wrong chain id {chain_id}; expected {CHAIN_ID}"
                )
            self._maintenance_chain_verified = True

        unseeded = frontier == anchor_block + 1 and frontier_hash is None
        end = anchor_block if unseeded else frontier - 1
        start = max(0, frontier - PARTICIPANT_SCAN_CHUNK)
        numbers = list(dict.fromkeys(
            (anchor_block, end, start)
            if unseeded
            else (anchor_block, frontier, end, start)
        ))

        def read_headers() -> dict[int, dict[str, Any]]:
            raw = self._maintenance_rpc.batch([
                ("eth_getBlockByNumber", [hex(number), False])
                for number in numbers
            ])
            headers: dict[int, dict[str, Any]] = {}
            for number, header in zip(numbers, raw):
                if not isinstance(header, dict) or not header.get("hash"):
                    raise RpcError(f"participant-history block {number} is unavailable")
                headers[number] = header
            return headers

        before = read_headers()
        if str(before[anchor_block]["hash"]).lower() != anchor_hash:
            raise ReorgDetected(f"participant-history anchor block {anchor_block} changed")
        if (
            frontier_hash is not None
            and str(before[frontier]["hash"]).lower() != frontier_hash
        ):
            raise ReorgDetected(f"participant-history frontier block {frontier} changed")
        logs = self._participant_history_logs(
            selection,
            start,
            end,
            rpc_client=self._maintenance_rpc,
        )
        for log in logs:
            if not isinstance(log, dict):
                raise RpcError("participant-history lifecycle log is malformed")
            block = _hex_int(log.get("blockNumber") or 0)
            if log.get("removed") is True or not start <= block <= end:
                raise ReorgDetected("participant-history returned removed or out-of-range data")
        candidate_blocks: dict[tuple[str, int, int], int] = {}
        for log in logs:
            topics = [str(topic).lower() for topic in (log.get("topics") or [])]
            if len(topics) < 4:
                continue
            key = (
                _topic_address(topics[1]),
                _signed_topic(topics[2]),
                _signed_topic(topics[3]),
            )
            candidate_blocks[key] = max(
                candidate_blocks.get(key, 0),
                _hex_int(log.get("blockNumber") or 0),
            )
        ordered_candidates = sorted(
            candidate_blocks,
            key=lambda key: candidate_blocks[key],
            reverse=True,
        )
        candidates_truncated = len(ordered_candidates) > MAX_PARTICIPANT_RANGES
        candidate_keys = ordered_candidates[:MAX_PARTICIPANT_RANGES]
        candidate_results = self._rpc_batches(
            [
                (
                    "eth_call",
                    [{
                        "to": selection.pool.address,
                        "data": _position_call(owner, lower, upper),
                    }, hex(anchor_block)],
                )
                for owner, lower, upper in candidate_keys
            ],
            rpc_client=self._maintenance_rpc,
        )
        active_states: dict[tuple[str, int, int], dict[str, int]] = {}
        for key, raw in zip(candidate_keys, candidate_results):
            words = _words(raw)
            if len(words) < 5:
                raise RpcError(
                    f"participant-history positions response is malformed for {key}"
                )
            state = {
                "liquidity": int(words[0], 16),
                "fee_growth0_last": int(words[1], 16),
                "fee_growth1_last": int(words[2], 16),
                "owed0": int(words[3], 16),
                "owed1": int(words[4], 16),
            }
            if state["liquidity"] or state["owed0"] or state["owed1"]:
                active_states[key] = state
        timestamps = self._timestamps(logs, rpc_client=self._maintenance_rpc)
        historical_events = self._derive_lp_events(
            selection, logs, timestamps, historical=True
        )
        after = read_headers()
        for number in numbers:
            if str(after[number]["hash"]).lower() != str(before[number]["hash"]).lower():
                raise ReorgDetected(
                    f"participant-history block {number} changed during the scan"
                )

        with selection.lock:
            if (
                selection.history_generation != generation
                or selection.history_floor != frontier
                or selection.needs_seed
            ):
                return False
            for key, state in active_states.items():
                selection.participant_state.setdefault(key, state)
            self._remember_participants(
                selection, logs, include=set(active_states)
            )
            if candidates_truncated:
                selection.participant_ranges_truncated = True
            self._merge_lp_events(selection, historical_events)
            selection.history_floor = start
            selection.history_frontier_hash = str(after[start]["hash"]).lower()
            selection.history_complete = start == 0
            selection.history_error = None
            selection.snapshot_dirty = True
            self._selection_wake.set()
            return True

    def _rpc_batches(
        self,
        calls: Iterable[tuple[str, list[Any]]],
        *,
        rpc_client: _Rpc | None = None,
    ) -> list[Any]:
        specifications = list(calls)
        output: list[Any] = []
        client = rpc_client or self.rpc
        for offset in range(0, len(specifications), 100):
            output.extend(client.batch(specifications[offset : offset + 100]))
        return output

    def _refresh_participants(
        self,
        selection: _Selection,
        block_tag: str,
        core: dict[str, Any],
        touched: Iterable[tuple[str, int, int]] = (),
        tick_path: Iterable[int] = (),
        rpc_client: _Rpc | None = None,
    ) -> set[tuple[str, int, int]]:
        """Refresh bounded position claims off-lock at one pinned block."""
        if selection.pool.kind != "v3":
            return set()
        block = _hex_int(block_tag)
        client = rpc_client or self.rpc
        with selection.lock:
            if selection.needs_seed or selection.cursor != block:
                return set()
            generation = selection.history_generation
            pool_address = selection.pool.address
            all_keys = set(selection.participant_ranges)
            if not all_keys:
                selection.position_state.clear()
                selection.participant_refresh_block = block
                selection.participant_refresh_error = None
                return set()
            resolved_keys = {
                key
                for key, state in selection.participant_state.items()
                if "claim0" in state
            }
            changed_keys = {
                key
                for key in all_keys
                if selection.participant_ranges[key]
                > int(selection.participant_state.get(key, {}).get("block", -1))
            }
            ordered_touched = sorted(
                (set(touched) & all_keys) | changed_keys,
                key=lambda key: (
                    key[0] == selection.owner,
                    selection.participant_ranges[key],
                ),
                reverse=True,
            )[:MAX_PARTICIPANT_REFRESH_PER_HEAD]
            touched_keys = set(ordered_touched)
            pending = sorted(
                all_keys - resolved_keys - touched_keys,
                key=lambda key: (
                    key[0] == selection.owner,
                    selection.participant_ranges[key],
                ),
                reverse=True,
            )
            room = max(
                0, MAX_PARTICIPANT_REFRESH_PER_HEAD - len(ordered_touched)
            )
            refresh_keys = [*ordered_touched, *pending[:room]]
            old_tick_value = (
                (selection.snapshot.get("spot") or {}).get("tick")
                if selection.snapshot else None
            )
            old_tick = (
                int(old_tick_value)
                if old_tick_value is not None else int(core["tick"])
            )
            current_tick = int(core["tick"])
            path = [old_tick, *[int(value) for value in tick_path], current_tick]
            crossed = {
                boundary
                for _owner, lower, upper in resolved_keys
                for boundary in (lower, upper)
                if any(
                    min(before, after) < boundary <= max(before, after)
                    for before, after in zip(path, path[1:])
                )
            }
            needed_ticks = crossed | {
                boundary
                for _owner, lower, upper in refresh_keys
                for boundary in (lower, upper)
            }
            new_owners = sorted({
                owner
                for owner, _lower, _upper in refresh_keys
                if owner not in selection.participant_kinds
            })

        global0_raw, global1_raw = client.batch([
            (
                "eth_call",
                [{
                    "to": pool_address,
                    "data": FEE_GROWTH_GLOBAL0_SELECTOR,
                }, block_tag],
            ),
            (
                "eth_call",
                [{
                    "to": pool_address,
                    "data": FEE_GROWTH_GLOBAL1_SELECTOR,
                }, block_tag],
            ),
        ])
        global0, global1 = _hex_int(global0_raw), _hex_int(global1_raw)
        position_results = self._rpc_batches(
            [
                (
                    "eth_call",
                    [{
                        "to": pool_address,
                        "data": _position_call(owner, lower, upper),
                    }, block_tag],
                )
                for owner, lower, upper in refresh_keys
            ],
            rpc_client=client,
        )
        ordered_ticks = sorted(needed_ticks)
        tick_results = self._rpc_batches(
            [
                (
                    "eth_call",
                    [{
                        "to": pool_address,
                        "data": TICKS_SELECTOR + _encode_signed(boundary),
                    }, block_tag],
                )
                for boundary in ordered_ticks
            ],
            rpc_client=client,
        )
        code_results = self._rpc_batches(
            [("eth_getCode", [owner, block_tag]) for owner in new_owners],
            rpc_client=client,
        )

        refreshed: dict[tuple[str, int, int], dict[str, int]] = {}
        for key, raw in zip(refresh_keys, position_results):
            words = _words(raw)
            if len(words) < 5:
                raise RpcError(f"positions response is malformed for {key}")
            refreshed[key] = {
                "liquidity": int(words[0], 16),
                "fee_growth0_last": int(words[1], 16),
                "fee_growth1_last": int(words[2], 16),
                "owed0": int(words[3], 16),
                "owed1": int(words[4], 16),
            }
        refreshed_ticks: dict[int, tuple[int, int]] = {}
        for boundary, raw in zip(ordered_ticks, tick_results):
            words = _words(raw)
            if len(words) < 4:
                raise RpcError(f"tick fee state is malformed at {boundary}")
            refreshed_ticks[boundary] = (
                int(words[2], 16), int(words[3], 16),
            )
        refreshed_kinds: dict[str, str] = {}
        for owner, code in zip(new_owners, code_results):
            normalized = str(code or "").lower()
            if not re.fullmatch(r"0x(?:[0-9a-f]{2})*", normalized):
                raise RpcError(f"owner bytecode response is malformed for {owner}")
            refreshed_kinds[owner] = (
                "manager_aggregate"
                if normalized not in {"0x", "0x0", "0x00"}
                else "direct_pool_position"
            )

        with selection.lock:
            if (
                selection.needs_seed
                or selection.cursor != block
                or selection.history_generation != generation
            ):
                return set()
            selection.participant_state.update(refreshed)
            selection.participant_tick_fees.update(refreshed_ticks)
            selection.participant_kinds.update(refreshed_kinds)
            claimable_keys = resolved_keys | set(refreshed)
            for (
                owner, lower, upper
            ), state in selection.participant_state.items():
                if (owner, lower, upper) not in claimable_keys:
                    continue
                lower_fees = selection.participant_tick_fees.get(lower)
                upper_fees = selection.participant_tick_fees.get(upper)
                if lower_fees is None or upper_fees is None:
                    continue
                claim0, claim1, lazy0, lazy1 = _v3_fee_claim(
                    int(state["liquidity"]),
                    int(state["fee_growth0_last"]),
                    int(state["fee_growth1_last"]),
                    int(state["owed0"]),
                    int(state["owed1"]),
                    global0,
                    global1,
                    lower_fees[0],
                    lower_fees[1],
                    upper_fees[0],
                    upper_fees[1],
                    current_tick,
                    lower,
                    upper,
                )
                state["claim0"] = claim0
                state["claim1"] = claim1
                state["lazy0"] = lazy0
                state["lazy1"] = lazy1
                state["block"] = block
            selection.position_state = {
                (lower, upper): (
                    int(state["liquidity"]),
                    int(state["owed0"]),
                    int(state["owed1"]),
                )
                for (
                    owner, lower, upper
                ), state in selection.participant_state.items()
                if owner == selection.owner
                and "claim0" in state
                and (
                    int(state["liquidity"])
                    or int(state["claim0"])
                    or int(state["claim1"])
                )
            }
            selection.participant_refresh_block = block
            selection.participant_refresh_error = None
            return set(refreshed)

    def _refresh_selected_participants(
        self, selection: _Selection,
    ) -> bool:
        if not selection.lock.acquire(blocking=False):
            return False
        try:
            if (
                selection.pool.kind != "v3"
                or selection.needs_seed
                or selection.cursor <= 0
                or not selection.participant_ranges
                or selection.core is None
                or selection.core_block != selection.cursor
                or selection.core_hash != selection.cursor_hash
            ):
                return False
            block = selection.cursor
            core = dict(selection.core)
            needs_refresh = (
                selection.participant_refresh_block != block
                or any(
                    observed_block
                    > int(selection.participant_state.get(key, {}).get("block", -1))
                    for key, observed_block
                    in selection.participant_ranges.items()
                )
            )
            if not needs_refresh:
                return False
            refresh_from = selection.participant_refresh_block
            refresh_from = refresh_from if refresh_from is not None else -1
            tick_path = [
                int(point["tick"])
                for point in selection.tracking
                if int(point["block"]) >= refresh_from
            ]
            timestamp = int(
                (selection.snapshot or {}).get("block_timestamp")
                or selection.created_at
            )
        finally:
            selection.lock.release()
        self._refresh_participants(
            selection,
            hex(block),
            core,
            rpc_client=self._maintenance_rpc,
            tick_path=tick_path,
        )
        with selection.lock:
            if (
                selection.needs_seed
                or selection.cursor != block
                or selection.participant_refresh_block != block
            ):
                return False
            baseline_keys = {
                key
                for key, state in selection.participant_state.items()
                if "claim0" in state
                and key not in selection.participant_accounting
            }
            self._establish_accounting_baselines(
                selection, baseline_keys, core, block, timestamp,
            )
            selection.snapshot_dirty = True
        self._selection_wake.set()
        return True

    def _establish_accounting_baselines(
        self,
        selection: _Selection,
        keys: Iterable[tuple[str, int, int]],
        core: dict[str, Any],
        block: int,
        timestamp: int,
    ) -> None:
        sqrt = int(core["sqrt"])
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        price = _price_from_sqrt(
            sqrt, metadata0.decimals if metadata0 else None,
            metadata1.decimals if metadata1 else None,
        )
        for key in keys:
            if key in selection.participant_accounting:
                continue
            state = selection.participant_state.get(key)
            if state is None or "claim0" not in state:
                continue
            _owner, lower, upper = key
            principal0, principal1 = principal_raw(
                int(state["liquidity"]), sqrt, lower, upper
            )
            claim0, claim1 = int(state["claim0"]), int(state["claim1"])
            selection.participant_accounting[key] = {
                "baseline_block": block,
                "baseline_timestamp": timestamp,
                "baseline_claim0": claim0,
                "baseline_claim1": claim1,
                "baseline_equity0": principal0 + claim0,
                "baseline_equity1": principal1 + claim1,
                "baseline_equity_usd": self._basket_value_usd(
                    selection, principal0 + claim0, principal1 + claim1, price
                ),
                "deposited_usd": 0.0,
                "collected_usd": 0.0,
                "deposited0": 0,
                "deposited1": 0,
                "burn_principal0": 0,
                "burn_principal1": 0,
                "collected0": 0,
                "collected1": 0,
                "status": "valid",
            }

    def _apply_accounting_logs(
        self,
        selection: _Selection,
        logs: Iterable[dict[str, Any]],
    ) -> None:
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        price = (selection.snapshot or {}).get("spot", {}).get("price_token1_per_token0")
        for log in logs:
            topics = [str(topic).lower() for topic in (log.get("topics") or [])]
            words = _words(log.get("data"))
            if topics and topics[0] == V3_SWAP_TOPIC and len(words) >= 5:
                price = _price_from_sqrt(
                    int(words[2], 16), metadata0.decimals if metadata0 else None,
                    metadata1.decimals if metadata1 else None,
                )
                continue
            if len(topics) < 4 or topics[0] not in {
                V3_MINT_TOPIC,
                V3_BURN_TOPIC,
                V3_COLLECT_TOPIC,
            }:
                continue
            key = (
                _topic_address(topics[1]),
                _signed_topic(topics[2]),
                _signed_topic(topics[3]),
            )
            accounting = selection.participant_accounting.get(key)
            block = _hex_int(log.get("blockNumber") or 0)
            if accounting is None or block <= int(accounting["baseline_block"]):
                continue
            cashflow_field = None
            if topics[0] == V3_MINT_TOPIC and len(words) >= 4:
                accounting["deposited0"] += int(words[2], 16)
                accounting["deposited1"] += int(words[3], 16)
                cashflow_field = "deposited_usd"
                amount0, amount1 = int(words[2], 16), int(words[3], 16)
            elif topics[0] == V3_BURN_TOPIC and len(words) >= 3:
                accounting["burn_principal0"] += int(words[1], 16)
                accounting["burn_principal1"] += int(words[2], 16)
            elif topics[0] == V3_COLLECT_TOPIC and len(words) >= 3:
                accounting["collected0"] += int(words[1], 16)
                accounting["collected1"] += int(words[2], 16)
                cashflow_field = "collected_usd"
                amount0, amount1 = int(words[1], 16), int(words[2], 16)
            if cashflow_field is not None:
                value = self._basket_value_usd(selection, amount0, amount1, price)
                prior = accounting[cashflow_field]
                accounting[cashflow_field] = (
                    prior + value if prior is not None and value is not None else None
                )

    @staticmethod
    def _invalidate_participant_accounting(
        selection: _Selection,
        reason: str,
    ) -> None:
        """Make an interval discontinuity visible before any reseed succeeds."""
        selection.participant_accounting.clear()
        selection.accounting_generation += 1
        selection.accounting_reset_reason = reason
        pending_positions = len(selection.participant_ranges)
        selection.participant_state.clear()
        selection.participant_tick_fees.clear()
        selection.position_state.clear()
        if selection.snapshot is None:
            return
        snapshot = deepcopy(selection.snapshot)
        for row in snapshot.get("participants") or []:
            row["fees_earned_usd"] = None
            row["fees_earned0"] = None
            row["fees_earned1"] = None
            row["pnl_usd"] = None
            row["pnl_since_block"] = None
            row["pnl_since_timestamp"] = None
            row["accounting_status"] = "baseline_invalidated"
        coverage = snapshot.setdefault("coverage", {})
        coverage["accounting"] = {
            "state": "baseline_invalidated",
            "basis": (
                "marked LP equity plus collections minus deposits minus starting "
                "equity; fees exclude burned principal; gas excluded"
            ),
            "claimable_semantics": (
                "total chain claim including lazy fees; may include removed principal"
            ),
            "since_block": None,
            "since_timestamp": None,
            "through_block": selection.cursor or None,
            "valid_positions": 0,
            "pending_positions": pending_positions,
            "gas_included": False,
            "reset_reason": reason,
        }
        selection.snapshot = snapshot

    def _viewport_words(self, selection: _Selection, tick: int) -> tuple[int, ...]:
        spacing = int(selection.pool.tick_spacing or 0)
        if spacing <= 0:
            return ()
        current = (tick // spacing) >> 8
        half = MAX_BITMAP_WORDS // 2
        first = current - half
        last = first + MAX_BITMAP_WORDS - 1
        if selection.position_state:
            boundaries = [value // spacing >> 8 for item in selection.position_state for value in item]
            wanted_first = min([current, *boundaries])
            wanted_last = max([current, *boundaries])
            if wanted_last - wanted_first + 1 <= MAX_BITMAP_WORDS:
                first = min(first, wanted_first)
                last = max(last, wanted_last)
                if last - first + 1 > MAX_BITMAP_WORDS:
                    if first < wanted_first:
                        first = last - MAX_BITMAP_WORDS + 1
                    else:
                        last = first + MAX_BITMAP_WORDS - 1
        return tuple(range(first, last + 1))

    def _load_curve_state(
        self,
        selection: _Selection,
        tick: int,
        block_tag: str,
        words: tuple[int, ...] | None = None,
    ) -> bool:
        if selection.pool.kind not in {"v3", "v4"}:
            return True
        words = self._viewport_words(selection, tick) if words is None else words
        if not words:
            return True
        pool = selection.pool
        spacing = int(pool.tick_spacing or 0)
        block = _hex_int(block_tag)
        can_reuse = (
            selection.curve_state_block == block
            or (
                selection.curve_dirty_from_block is None
                and selection.curve_verified_through_block is not None
                and selection.curve_verified_through_block >= block
            )
        )
        reused_words = (
            set(selection.curve_words).intersection(words)
            if can_reuse else set()
        )
        load_words = tuple(word for word in words if word not in reused_words)
        if pool.kind == "v3":
            bitmap_calls = [
                (
                    "eth_call",
                    [{
                        "to": pool.address,
                        "data": TICK_BITMAP_SELECTOR + _encode_signed(word),
                    }, block_tag],
                )
                for word in load_words
            ]
        else:
            bitmap_calls = [
                (
                    "eth_call",
                    [{
                        "to": STATE_VIEW,
                        "data": (
                            SV_TICK_BITMAP_SELECTOR
                            + pool.id[2:]
                            + _encode_signed(word)
                        ),
                    }, block_tag],
                )
                for word in load_words
            ]
        bitmaps = self._pinned_state_batch(bitmap_calls, block_tag)
        initialized: list[int] = []
        for word, raw in zip(load_words, bitmaps):
            bitmap_words = _words(raw)
            if not bitmap_words:
                raise RpcError(f"tick bitmap is malformed at word {word}")
            bitmap = int(bitmap_words[0], 16)
            while bitmap:
                lowest = bitmap & -bitmap
                bit = lowest.bit_length() - 1
                initialized_tick = ((word << 8) + bit) * spacing
                if not -MAX_TICK <= initialized_tick <= MAX_TICK:
                    raise RpcError(
                        f"initialized tick outside TickMath domain: {initialized_tick}"
                    )
                initialized.append(initialized_tick)
                bitmap ^= lowest

        # A newly selected visible pool should not sit behind the thousands of
        # tick reads belonging to an iframe which was closed or switched.
        if self._selection_superseded(selection):
            return False
        calls: list[tuple[str, list[Any]]] = []
        for initialized_tick in initialized:
            if pool.kind == "v3":
                data = TICKS_SELECTOR + _encode_signed(initialized_tick)
                target = pool.address
            else:
                data = (
                    SV_TICK_INFO_SELECTOR
                    + pool.id[2:]
                    + _encode_signed(initialized_tick)
                )
                target = STATE_VIEW
            calls.append(("eth_call", [{"to": target, "data": data}, block_tag]))
        results = self._pinned_state_batch(calls, block_tag)
        if self._selection_superseded(selection):
            return False
        tick_net = {
            initialized_tick: net
            for initialized_tick, net in selection.tick_net.items()
            if (initialized_tick // spacing >> 8) in reused_words
        }
        for initialized_tick, raw in zip(initialized, results):
            info = _words(raw)
            if len(info) < 2:
                raise RpcError(f"tick info is malformed at {initialized_tick}")
            gross = int(info[0], 16)
            net = _signed_word(info[1])
            if gross or net:
                tick_net[initialized_tick] = net
        selection.tick_net = tick_net
        selection.curve_words = words
        return True

    def _timestamps(
        self,
        logs: Iterable[dict[str, Any]],
        rpc_client: _Rpc | None = None,
    ) -> dict[int, int]:
        rows = list(logs)
        numbers = sorted({
            _hex_int(log.get("blockNumber") or 0) for log in rows
        })
        if not numbers:
            return {}
        results: list[Any] = []
        client = rpc_client or self.rpc
        for start in range(0, len(numbers), 100):
            results.extend(client.batch([
                ("eth_getBlockByNumber", [hex(number), False])
                for number in numbers[start : start + 100]
            ]))
        output: dict[int, int] = {}
        hashes: dict[int, str] = {}
        for number, header in zip(numbers, results):
            if (
                not isinstance(header, dict)
                or header.get("timestamp") is None
                or not _BYTES32_RE.fullmatch(
                    str(header.get("hash") or "").lower()
                )
            ):
                raise RpcError(f"block {number} timestamp/hash is unavailable")
            output[number] = _hex_int(header["timestamp"])
            hashes[number] = str(header["hash"]).lower()
        for log in rows:
            number = _hex_int(log.get("blockNumber") or 0)
            log_hash = str(log.get("blockHash") or "").lower()
            if log.get("removed") is True or log_hash != hashes[number]:
                raise ReorgDetected(
                    f"event log does not belong to canonical block {number}"
                )
        return output

    def _swap_seed_boundary(
        self, header: dict[str, Any], rpc_client: _Rpc | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Find a bounded, timestamp-verified start for the one-minute window."""
        block = _hex_int(header["number"])
        target_timestamp = _hex_int(header["timestamp"]) - 60
        distance = min(block, SWAP_LOOKBACK_BLOCKS)
        while True:
            start = max(0, block - distance)
            start_header = (rpc_client or self.rpc).call(
                "eth_getBlockByNumber", [hex(start), False],
            )
            if (
                not isinstance(start_header, dict)
                or start_header.get("timestamp") is None
                or not start_header.get("hash")
            ):
                raise RpcError(f"swap coverage block {start} is unavailable")
            if (
                _hex_int(start_header["timestamp"]) <= target_timestamp
                or start == 0
                or distance >= MAX_SWAP_LOOKBACK_BLOCKS
            ):
                return start, start_header
            distance = min(MAX_SWAP_LOOKBACK_BLOCKS, max(distance + 1, distance * 2))

    @staticmethod
    def _record_tracking(
        selection: _Selection,
        block: int,
        timestamp: int,
        price: float | None,
        active_liquidity: int,
    ) -> None:
        point = {
            "block": block,
            "timestamp": timestamp,
            "price_token1_per_token0": price,
            "active_liquidity": str(active_liquidity),
        }
        by_block = {
            int(existing["block"]): existing for existing in selection.tracking
        }
        by_block[block] = point
        selection.tracking = deque(
            [
                by_block[number]
                for number in sorted(by_block)[-MAX_TRACKING_POINTS:]
            ],
            maxlen=MAX_TRACKING_POINTS,
        )

    @staticmethod
    def _current_event_pool_id(event: Mapping[str, Any]) -> str:
        pool_id = str(event.get("pool_id") or "").lower()
        if pool_id:
            return pool_id
        pool = event.get("pool")
        if not isinstance(pool, Mapping):
            return ""
        return str(pool.get("id") or pool.get("address") or "").lower()

    @staticmethod
    def _current_event_identity(
        event: Mapping[str, Any],
    ) -> tuple[str, str, int]:
        return (
            str(event["block_hash"]).lower(),
            str(event["tx_hash"]).lower(),
            int(event["log_index"]),
        )

    @staticmethod
    def _current_shape_change(event: Mapping[str, Any]) -> bool:
        if str(event.get("kind") or "") not in {"add", "remove"}:
            return False
        try:
            return int(event.get("liquidity_delta") or 0) != 0
        except (TypeError, ValueError):
            return False

    @staticmethod
    def _merge_swap_row(selection: _Selection, row: dict[str, Any]) -> bool:
        rows = list(selection.swaps)
        for index, existing in enumerate(rows):
            if existing["id"] != row["id"]:
                continue
            merged = {
                **existing,
                **{
                    key: value
                    for key, value in row.items()
                    if value is not None
                },
            }
            if existing.get("qualification") == "canonical_exact_logs":
                merged["qualification"] = "canonical_exact_logs"
            if merged == existing:
                return False
            rows[index] = merged
            selection.swaps = deque(rows, maxlen=MAX_SWAPS)
            return True
        ordered = sorted(
            [*rows, row],
            key=lambda item: (
                int(item["block"]),
                int(item.get("transaction_index") or 0),
                int(item.get("log_index") or 0),
            ),
        )
        while len(ordered) > MAX_SWAPS:
            dropped = ordered.pop(0)
            selection.swaps_truncated_through_timestamp = max(
                int(dropped["timestamp"]),
                selection.swaps_truncated_through_timestamp or 0,
            )
        selection.swaps = deque(ordered, maxlen=MAX_SWAPS)
        return True

    def _current_lp_event(
        self,
        selection: _Selection,
        event: Mapping[str, Any],
        *,
        current: bool = True,
    ) -> dict[str, Any] | None:
        kind = str(event.get("kind") or "")
        if kind not in {"swap", "add", "remove", "checkpoint", "collect"}:
            return None
        data = event.get("data")
        data = data if isinstance(data, Mapping) else {}
        owner = event.get("owner") or event.get("custody") or data.get("sender")
        owner = str(owner).lower() if owner else None
        amount0 = event.get("amount0")
        amount1 = event.get("amount1")
        raw0 = int(amount0) if amount0 is not None else None
        raw1 = int(amount1) if amount1 is not None else None
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        sqrt_value = event.get("sqrt_price_x96")
        sqrt = int(sqrt_value) if sqrt_value is not None else None
        tick_value = event.get("tick")
        tick = int(tick_value) if tick_value is not None else None
        tx_hash = str(event["tx_hash"]).lower()
        log_index = int(event["log_index"])
        return {
            "id": f"{tx_hash}:{log_index}",
            "block": int(event["block_number"]),
            "timestamp": int(event["timestamp"]),
            "tx_hash": tx_hash,
            "kind": kind,
            "owner": owner,
            "lo": (
                int(event["tick_lower"])
                if event.get("tick_lower") is not None else None
            ),
            "hi": (
                int(event["tick_upper"])
                if event.get("tick_upper") is not None else None
            ),
            "liquidity_delta": str(event.get("liquidity_delta") or 0),
            "amount0": _format_units(raw0, decimals0) if raw0 is not None else None,
            "amount1": _format_units(raw1, decimals1) if raw1 is not None else None,
            "amount0_raw": str(raw0) if raw0 is not None else None,
            "amount1_raw": str(raw1) if raw1 is not None else None,
            "share_before_pct": None,
            "share_after_pct": None,
            "tick_before": None,
            "tick_after": tick,
            "price_before": None,
            "price_after": (
                _price_from_sqrt(sqrt, decimals0, decimals1)
                if sqrt is not None else None
            ),
            "ours": selection.owner is not None and selection.owner == owner,
            "_current_observation": current,
            "_order": (
                int(event["block_number"]),
                int(event.get("tx_index") or 0),
                log_index,
            ),
        }

    def _apply_current_events(
        self,
        selection: _Selection,
        events: Sequence[Mapping[str, Any]],
    ) -> bool:
        changed = False
        lp_events: list[dict[str, Any]] = []
        ordered = sorted(
            events,
            key=lambda event: (
                int(event["block_number"]),
                int(event.get("tx_index") or 0),
                int(event["log_index"]),
            ),
        )
        for event in ordered:
            if (
                self._current_event_pool_id(event) != selection.pool.id
                or str(event.get("protocol") or "") != selection.pool.kind
            ):
                continue
            identity = self._current_event_identity(event)
            previous_event = selection.current_event_identities.get(identity)
            if previous_event == event:
                continue
            current_event = dict(event)
            is_new = previous_event is None
            selection.current_event_identities[identity] = current_event
            block = int(event["block_number"])
            kind = str(event.get("kind") or "")
            if is_new and self._current_shape_change(event):
                state_block = selection.curve_state_block or -1
                if block > state_block:
                    selection.curve_dirty_from_block = min(
                        block,
                        selection.curve_dirty_from_block or block,
                    )
            if kind == "swap":
                amount0 = int(event["amount0"])
                amount1 = int(event["amount1"])
                sqrt_value = event.get("sqrt_price_x96")
                liquidity_value = event.get("liquidity")
                sqrt = int(sqrt_value) if sqrt_value is not None else None
                active = (
                    int(liquidity_value)
                    if liquidity_value is not None else None
                )
                fee_value = event.get("fee_ppm")
                fee_ppm = int(fee_value) if fee_value is not None else None
                if fee_ppm is not None and not 0 <= fee_ppm <= 1_000_000:
                    fee_ppm = None
                if selection.pool.kind == "v4" and fee_ppm is not None:
                    selection.current_fee = fee_ppm
                metadata0 = self.universe.tokens.get(selection.pool.token0)
                metadata1 = self.universe.tokens.get(selection.pool.token1)
                decimals0 = metadata0.decimals if metadata0 else None
                decimals1 = metadata1.decimals if metadata1 else None
                tx_hash = str(event["tx_hash"]).lower()
                row = {
                    "id": f"{tx_hash}:{int(event['log_index'])}",
                    "block": block,
                    "block_hash": str(event["block_hash"]).lower(),
                    "qualification": "verified_current_observation",
                    "timestamp": int(event["timestamp"]),
                    "transaction_index": int(event.get("tx_index") or 0),
                    "log_index": int(event["log_index"]),
                    "amount0": _format_units(amount0, decimals0),
                    "amount1": _format_units(amount1, decimals1),
                    "amount0_raw": str(amount0),
                    "amount1_raw": str(amount1),
                    "tick": (
                        int(event["tick"])
                        if event.get("tick") is not None else None
                    ),
                    "sqrt_price_x96": str(sqrt) if sqrt is not None else None,
                    "active_liquidity": str(active) if active is not None else None,
                    "fee_ppm": fee_ppm,
                    "tx_hash": tx_hash,
                }
                changed = self._merge_swap_row(selection, row) or changed
                if sqrt is not None and active is not None:
                    self._record_tracking(
                        selection,
                        block,
                        int(event["timestamp"]),
                        _price_from_sqrt(sqrt, decimals0, decimals1),
                        active,
                    )
                    changed = True
            current_lp = self._current_lp_event(selection, event)
            if current_lp is not None:
                lp_events.append(current_lp)
                changed = True
            if (
                is_new
                and selection.pool.kind == "v3"
                and kind in {"add", "remove", "checkpoint", "collect"}
                and event.get("custody")
                and event.get("tick_lower") is not None
                and event.get("tick_upper") is not None
            ):
                owner = str(event["custody"]).lower()
                lower = int(event["tick_lower"])
                upper = int(event["tick_upper"])
                participant_key = (owner, lower, upper)
                if (
                    participant_key in selection.participant_ranges
                    or len(selection.participant_ranges) < MAX_PARTICIPANT_RANGES
                ):
                    selection.participant_ranges[participant_key] = max(
                        block,
                        selection.participant_ranges.get(participant_key, 0),
                    )
                else:
                    selection.participant_ranges_truncated = True
                if owner == selection.owner:
                    own_key = (lower, upper)
                    if (
                        own_key in selection.ranges
                        or len(selection.ranges) < MAX_POSITION_RANGES
                    ):
                        selection.ranges[own_key] = max(
                            block, selection.ranges.get(own_key, 0)
                        )
                    else:
                        selection.ranges_truncated = True
                    if kind in {"add", "remove"}:
                        existing_state = selection.participant_state.get(
                            participant_key
                        )
                        if (
                            existing_state is not None
                            and int(existing_state.get("block", -1)) < block
                        ):
                            delta = int(event.get("liquidity_delta") or 0)
                            existing_state["liquidity"] = max(
                                0, int(existing_state["liquidity"]) + delta
                            )
                            old = selection.position_state.get(own_key)
                            if old is not None:
                                selection.position_state[own_key] = (
                                    existing_state["liquidity"], old[1], old[2]
                                )
                        selection.owner_liquidity_changes.append({
                            "block": block,
                            "timestamp": int(event["timestamp"]),
                            "log_index": int(event["log_index"]),
                        })
                changed = True
        if lp_events:
            self._merge_lp_events(selection, lp_events)
        return changed

    def _sync_current_events(
        self,
        selection: _Selection,
        through_block: int,
    ) -> bool:
        events: list[dict[str, Any]] = []
        revisions: dict[int, int] = {}
        first = last = None
        oldest = None
        with self._lock:
            epoch = self._current_observation_epoch
            observation_error = self._current_observation_error
            tail_first = tail_last = None
            for number, record in self._current_blocks.items():
                if number > through_block:
                    break
                if oldest is None:
                    oldest = number
                revision = record["revision"]
                if selection.current_event_revisions.get(number) != revision:
                    pool_events = record["events"].get(selection.pool.id)
                    if pool_events:
                        events.extend(pool_events.values())
                    revisions[number] = revision
                if record["events_observed"]:
                    if tail_last is None or number != tail_last["number"] + 1:
                        tail_first = record
                    tail_last = record
                else:
                    tail_first = tail_last = None
            if tail_first is not None:
                first = {
                    "number": tail_first["number"],
                    "timestamp": tail_first["timestamp"],
                }
                last = {"number": tail_last["number"], "hash": tail_last["hash"]}
        if selection.current_observation_epoch not in {0, epoch}:
            selection.current_event_error = observation_error or (
                "current event observation continuity changed"
            )
            raise ReorgDetected(selection.current_event_error)
        selection.current_observation_epoch = epoch
        before_coverage = (
            selection.current_event_start_block,
            selection.current_event_through_block,
            selection.current_event_through_hash,
            selection.curve_verified_through_block,
            selection.current_event_error,
        )
        changed = self._apply_current_events(selection, events) if events else False
        selection.current_event_revisions.update(revisions)
        if first is not None and last is not None:
            joins_prior = (
                selection.current_event_through_block is not None
                and first["number"] <= selection.current_event_through_block + 1
            )
            if not joins_prior:
                selection.current_event_start_block = first["number"]
                selection.current_event_start_timestamp = first["timestamp"]
            elif selection.current_event_start_block is None:
                selection.current_event_start_block = first["number"]
                selection.current_event_start_timestamp = first["timestamp"]
            selection.current_event_through_block = max(
                last["number"], selection.current_event_through_block or -1
            )
            if selection.current_event_through_block == last["number"]:
                selection.current_event_through_hash = last["hash"]
            selection.current_event_error = None

        if (
            selection.curve_state_block is not None
            and selection.curve_dirty_from_block is not None
        ):
            selection.curve_verified_through_block = min(
                selection.curve_verified_through_block
                or selection.curve_state_block,
                selection.curve_dirty_from_block - 1,
            )
        changed = changed or before_coverage != (
            selection.current_event_start_block,
            selection.current_event_through_block,
            selection.current_event_through_hash,
            selection.curve_verified_through_block,
            selection.current_event_error,
        )
        if oldest is not None:
            while (
                selection.current_event_revisions
                and next(iter(selection.current_event_revisions)) < oldest
            ):
                selection.current_event_revisions.popitem(last=False)
            expired = [
                identity for identity, event in selection.current_event_identities.items()
                if int(event["block_number"]) < oldest
            ]
            for identity in expired:
                del selection.current_event_identities[identity]
        if changed:
            selection.snapshot_dirty = True
        return changed

    def _append_owner_liquidity_changes(
        self,
        selection: _Selection,
        logs: Iterable[dict[str, Any]],
        timestamps: dict[int, int] | None = None,
    ) -> None:
        if selection.pool.kind != "v3" or not selection.owner:
            return
        changes = []
        for log in logs:
            topics = [str(topic).lower() for topic in (log.get("topics") or [])]
            if (
                len(topics) > 1
                and topics[0] in {V3_MINT_TOPIC, V3_BURN_TOPIC}
                and topics[1][-40:] == selection.owner[2:]
            ):
                changes.append(log)
        timestamps = timestamps or self._timestamps(changes)
        existing = {
            (int(row["block"]), int(row["log_index"]))
            for row in selection.owner_liquidity_changes
        }
        for log in changes:
            block = _hex_int(log.get("blockNumber") or 0)
            log_index = _hex_int(log.get("logIndex") or 0)
            if (block, log_index) in existing:
                continue
            selection.owner_liquidity_changes.append({
                "block": block,
                "timestamp": timestamps[block],
                "log_index": log_index,
            })
            existing.add((block, log_index))

    def _append_swaps(
        self,
        selection: _Selection,
        logs: Iterable[dict[str, Any]],
        timestamps: dict[int, int] | None = None,
    ) -> None:
        swap_logs = []
        for log in logs:
            topics = [str(topic).lower() for topic in (log.get("topics") or [])]
            if not topics:
                continue
            expected = {
                "v2": V2_SWAP_TOPIC,
                "v3": V3_SWAP_TOPIC,
                "v4": V4_SWAP_TOPIC,
            }[selection.pool.kind]
            if topics[0] == expected:
                if (
                    selection.pool.kind != "v4"
                    or (len(topics) > 1 and topics[1] == selection.pool.id)
                ):
                    swap_logs.append(log)
        timestamps = timestamps or self._timestamps(swap_logs)
        token0 = self.universe.tokens.get(selection.pool.token0)
        token1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = token0.decimals if token0 else None
        decimals1 = token1.decimals if token1 else None
        for log in swap_logs:
            words = _words(log.get("data"))
            kind = selection.pool.kind
            sqrt_price: int | None = None
            active_liquidity: int | None = None
            fee_ppm = selection.current_fee
            if kind == "v3":
                if len(words) < 5:
                    raise RpcError("malformed V3 Swap log")
                amount0, amount1 = _signed_word(words[0]), _signed_word(words[1])
                sqrt_price = int(words[2], 16)
                active_liquidity = int(words[3], 16)
                tick = _signed_word(words[4])
            elif kind == "v4":
                if len(words) < 6:
                    raise RpcError("malformed V4 Swap log")
                amount0, amount1 = _signed_word(words[0]), _signed_word(words[1])
                sqrt_price = int(words[2], 16)
                active_liquidity = int(words[3], 16)
                tick = _signed_word(words[4], 24)
                fee_ppm = int(words[5], 16) & ((1 << 24) - 1)
                selection.current_fee = fee_ppm
            else:
                if len(words) < 4:
                    raise RpcError("malformed V2 Swap log")
                amount0 = int(words[0], 16) - int(words[2], 16)
                amount1 = int(words[1], 16) - int(words[3], 16)
                tick = None
            block = _hex_int(log.get("blockNumber") or 0)
            timestamp = timestamps[block]
            tx_hash = str(log.get("transactionHash") or "").lower()
            log_index = _hex_int(log.get("logIndex") or 0)
            row = {
                "id": f"{tx_hash}:{log_index}",
                "block": block,
                "block_hash": str(log.get("blockHash") or "").lower() or None,
                "qualification": "canonical_exact_logs",
                "timestamp": timestamp,
                "transaction_index": _hex_int(
                    log.get("transactionIndex") or 0
                ),
                "log_index": log_index,
                "amount0": _format_units(amount0, decimals0),
                "amount1": _format_units(amount1, decimals1),
                "amount0_raw": str(amount0),
                "amount1_raw": str(amount1),
                "tick": tick,
                "sqrt_price_x96": (
                    str(sqrt_price) if sqrt_price is not None else None
                ),
                "active_liquidity": (
                    str(active_liquidity)
                    if active_liquidity is not None else None
                ),
                "fee_ppm": fee_ppm,
                "tx_hash": tx_hash,
            }
            inserted = self._merge_swap_row(selection, row)
            if (
                inserted
                and sqrt_price is not None
                and active_liquidity is not None
            ):
                self._record_tracking(
                    selection,
                    block,
                    timestamp,
                    _price_from_sqrt(
                        sqrt_price, decimals0, decimals1,
                    ),
                    active_liquidity,
                )
        if timestamps:
            cutoff = max(timestamps.values()) - 3600
            while selection.swaps and int(selection.swaps[0]["timestamp"]) < cutoff:
                selection.swaps.popleft()

    def _derive_lp_events(
        self,
        selection: _Selection,
        logs: Iterable[dict[str, Any]],
        timestamps: dict[int, int],
        *,
        historical: bool = False,
    ) -> list[dict[str, Any]]:
        """Decode ordered canonical events; group range moves only in the view."""
        if selection.pool.kind != "v3":
            from .lp_market_protocols import decode_logs

            pool = selection.pool
            logs = list(logs)
            rows = decode_logs(
                logs,
                {pool.id: {
                    "id": pool.id, "address": pool.address, "protocol": pool.kind,
                    "token0": pool.token0, "token1": pool.token1, "fee_ppm": pool.fee_ppm,
                }},
                {
                    _hex_int(log["blockNumber"]): {
                        "hash": log["blockHash"],
                        "timestamp": timestamps[_hex_int(log["blockNumber"])],
                    }
                    for log in logs
                },
            )
            return [
                event for row in rows
                if (event := self._current_lp_event(selection, row, current=False)) is not None
            ]
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        snapshot = None if historical else selection.snapshot
        spot = (snapshot or {}).get("spot") or {}
        liquidity_view = (snapshot or {}).get("liquidity") or {}
        tick: int | None = (
            int(spot["tick"]) if spot.get("tick") is not None else None
        )
        sqrt: int | None = (
            int(spot["sqrt_price_x96"])
            if spot.get("sqrt_price_x96") not in (None, "0")
            else None
        )
        active: int | None = (
            int(liquidity_view["active"])
            if liquidity_view.get("active") is not None
            else None
        )
        position_liquidity: dict[tuple[str, int, int], int] = (
            {}
            if historical
            else {
                key: int(state["liquidity"])
                for key, state in selection.participant_state.items()
            }
        )

        def price(current_sqrt: int | None) -> float | None:
            return (
                _price_from_sqrt(current_sqrt, decimals0, decimals1)
                if current_sqrt is not None
                else None
            )

        expected_ours = {
            (selection.owner, lower, upper)
            for lower, upper in selection.ranges
        } if selection.owner else set()
        unknown_ours = historical

        def share(
            total: int | None,
            current_tick: int | None,
        ) -> float | None:
            if (
                historical
                or selection.owner is None
                or current_tick is None
                or selection.ranges_truncated
            ):
                return None
            owner_seed_covers_window = (
                selection.owner_range_seed_block is not None
                and selection.swap_coverage_start_block is not None
                and selection.swap_coverage_start_block
                <= selection.owner_range_seed_block
                <= selection.cursor
            )
            if not selection.history_complete and not owner_seed_covers_window:
                return None
            if unknown_ours or not expected_ours.issubset(position_liquidity):
                return None
            if total is None or total <= 0:
                return None
            ours = sum(
                liquidity
                for (owner, lower, upper), liquidity in position_liquidity.items()
                if owner == selection.owner
                and liquidity > 0
                and lower <= current_tick < upper
            )
            return float(Decimal(ours) * Decimal(100) / Decimal(total))

        events: list[dict[str, Any]] = []
        for log in logs:
            topics = [str(topic).lower() for topic in (log.get("topics") or [])]
            if not topics:
                continue
            topic = topics[0]
            if topic not in {
                V3_SWAP_TOPIC,
                V3_MINT_TOPIC,
                V3_BURN_TOPIC,
                V3_COLLECT_TOPIC,
            }:
                continue
            words = _words(log.get("data"))
            block = _hex_int(log.get("blockNumber") or 0)
            tx_hash = str(log.get("transactionHash") or "").lower()
            tx_index = _hex_int(log.get("transactionIndex") or 0)
            log_index = _hex_int(log.get("logIndex") or 0)
            timestamp = timestamps.get(block)
            if timestamp is None:
                raise RpcError(f"LP event block {block} timestamp is unavailable")
            tick_before = tick
            price_before = price(sqrt)
            share_before = share(active, tick)
            if topic == V3_SWAP_TOPIC:
                if len(words) < 5:
                    raise RpcError("malformed V3 Swap lifecycle log")
                amount0 = _signed_word(words[0])
                amount1 = _signed_word(words[1])
                sqrt = int(words[2], 16)
                active = int(words[3], 16)
                tick = _signed_word(words[4])
                owner = _topic_address(topics[1]) if len(topics) > 1 else None
                event = {
                    "id": f"{tx_hash}:{log_index}",
                    "block": block,
                    "timestamp": timestamp,
                    "tx_hash": tx_hash,
                    "kind": "swap",
                    "owner": owner,
                    "lo": None,
                    "hi": None,
                    "liquidity_delta": "0",
                    "amount0": _format_units(amount0, decimals0),
                    "amount1": _format_units(amount1, decimals1),
                    "amount0_raw": str(amount0),
                    "amount1_raw": str(amount1),
                    "share_before_pct": share_before,
                    "share_after_pct": share(active, tick),
                    "tick_before": tick_before,
                    "tick_after": tick,
                    "price_before": price_before,
                    "price_after": price(sqrt),
                    "ours": False,
                    "_order": (block, tx_index, log_index),
                }
                events.append(event)
                continue
            if len(topics) < 4:
                raise RpcError("malformed V3 LP lifecycle topics")
            owner = _topic_address(topics[1])
            lower, upper = _signed_topic(topics[2]), _signed_topic(topics[3])
            key = (owner, lower, upper)
            before_position = position_liquidity.get(key)
            if owner == selection.owner and before_position is None:
                share_before = None
                unknown_ours = True
            if topic == V3_MINT_TOPIC:
                if len(words) < 4:
                    raise RpcError("malformed V3 Mint lifecycle log")
                delta = int(words[1], 16)
                amount0, amount1 = int(words[2], 16), int(words[3], 16)
                kind = "add"
                if before_position is not None:
                    position_liquidity[key] = before_position + delta
                else:
                    position_liquidity[key] = delta
                if active is not None and tick is not None and lower <= tick < upper:
                    active += delta
            elif topic == V3_BURN_TOPIC:
                if len(words) < 3:
                    raise RpcError("malformed V3 Burn lifecycle log")
                delta = -int(words[0], 16)
                amount0, amount1 = int(words[1], 16), int(words[2], 16)
                kind = "remove" if delta else "checkpoint"
                if before_position is not None:
                    position_liquidity[key] = max(0, before_position + delta)
                if active is not None and tick is not None and lower <= tick < upper:
                    active = max(0, active + delta)
            else:
                if len(words) < 3:
                    raise RpcError("malformed V3 Collect lifecycle log")
                delta = 0
                amount0, amount1 = int(words[1], 16), int(words[2], 16)
                kind = "collect"
            events.append({
                "id": f"{tx_hash}:{log_index}",
                "block": block,
                "timestamp": timestamp,
                "tx_hash": tx_hash,
                "kind": kind,
                "owner": owner,
                "lo": lower,
                "hi": upper,
                "liquidity_delta": str(delta),
                "amount0": _format_units(amount0, decimals0),
                "amount1": _format_units(amount1, decimals1),
                "amount0_raw": str(amount0),
                "amount1_raw": str(amount1),
                "share_before_pct": share_before,
                "share_after_pct": share(active, tick),
                "tick_before": tick_before,
                "tick_after": tick,
                "price_before": price_before,
                "price_after": price(sqrt),
                "ours": selection.owner == owner,
                "_order": (block, tx_index, log_index),
            })
        return events


    @staticmethod
    def _merge_lp_events(
        selection: _Selection,
        events: Iterable[dict[str, Any]],
    ) -> None:
        by_id = {str(event["id"]): event for event in selection.lp_events}
        for event in events:
            identity = str(event["id"])
            previous = by_id.get(identity)
            if previous is None:
                by_id[identity] = event
                continue
            preferred, other = event, previous
            if previous.get("_current_observation") and not event.get("_current_observation"):
                preferred, other = previous, event
            by_id[identity] = {
                **other, **{key: value for key, value in preferred.items() if value is not None},
            }
        ordered = sorted(by_id.values(), key=lambda event: event["_order"])
        if len(ordered) > MAX_LP_EVENTS:
            selection.lp_events_truncated = True
            ordered = ordered[-MAX_LP_EVENTS:]
        selection.lp_events = deque(ordered, maxlen=MAX_LP_EVENTS)
        if ordered:
            selection.lp_event_coverage_start_block = int(ordered[0]["block"])
            selection.lp_event_coverage_start_timestamp = int(ordered[0]["timestamp"])

    def _record_core_tracking(
        self,
        selection: _Selection,
        header: dict[str, Any],
        core: dict[str, Any],
    ) -> None:
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        if selection.pool.kind == "v2":
            reserve0 = int(core["reserve0"])
            reserve1 = int(core["reserve1"])
            active = math.isqrt(reserve0 * reserve1)
            if reserve0 > 0 and decimals0 is not None and decimals1 is not None:
                with localcontext() as context:
                    context.prec = 72
                    value = (
                        Decimal(reserve1) / Decimal(reserve0)
                        * (Decimal(10) ** (decimals0 - decimals1))
                    )
                    price = float(value)
                if not math.isfinite(price) or price <= 0:
                    price = None
            else:
                price = None
        else:
            active = int(core["liquidity"])
            price = _price_from_sqrt(int(core["sqrt"]), decimals0, decimals1)
        self._record_tracking(
            selection,
            _hex_int(header["number"]),
            _hex_int(header["timestamp"]),
            price,
            active,
        )

    def _seed_selection(self, selection: _Selection) -> bool:
        header = self._head_header()
        block = _hex_int(header["number"])
        block_hash = str(header["hash"]).lower()
        block_tag = hex(block)
        core = self._read_core(selection, block_tag, seed=True)
        if self._selection_superseded(selection):
            return False
        selection.history_generation += 1
        selection.accounting_generation += 1
        selection.ranges.clear()
        selection.position_state.clear()
        selection.participant_ranges.clear()
        selection.participant_state.clear()
        selection.participant_tick_fees.clear()
        selection.participant_accounting.clear()
        selection.tick_net.clear()
        selection.curve_words = ()
        selection.curve_state_block = None
        selection.curve_state_hash = None
        selection.curve_verified_through_block = None
        selection.curve_dirty_from_block = None
        selection.swaps.clear()
        selection.swaps_truncated_through_timestamp = None
        selection.swap_coverage_start_block = None
        selection.swap_coverage_start_timestamp = None
        selection.swap_coverage_through_block = None
        selection.swap_coverage_through_hash = None
        selection.current_event_start_block = None
        selection.current_event_start_timestamp = None
        selection.current_event_through_block = None
        selection.current_event_through_hash = None
        selection.current_event_error = None
        selection.current_observation_epoch = 0
        selection.current_event_identities.clear()
        selection.current_event_revisions.clear()
        selection.owner_liquidity_changes.clear()
        selection.tracking.clear()
        selection.lp_events.clear()
        selection.lp_event_coverage_start_block = None
        selection.lp_event_coverage_start_timestamp = None
        selection.lp_events_truncated = False
        selection.ranges_truncated = False
        selection.owner_range_seed_block = None
        selection.participant_ranges_truncated = False
        selection.position_error = None
        selection.participant_refresh_error = None
        selection.participant_refresh_block = None
        selection.accounting_reset_reason = None
        selection.history_error = None
        selection.last_history_scan = 0.0
        selection.last_recent_history_scan = 0.0
        selection.recent_history_cursor = 0
        # Hydrate only the contiguous words needed to paint real initial depth.
        # Historical events, owner state, and metadata all stay off this lane.
        if selection.pool.kind in {"v3", "v4"}:
            words = self._viewport_words(selection, int(core["tick"]))
            if len(words) > 2:
                spacing = int(selection.pool.tick_spacing or 0)
                compressed_tick = int(core["tick"]) // spacing
                current_word = compressed_tick >> 8
                neighbor = (
                    current_word - 1
                    if (compressed_tick & 255) < 128
                    else current_word + 1
                )
                words = tuple(
                    word for word in words
                    if word in {current_word, neighbor}
                )
            if not self._load_curve_state(
                selection, int(core["tick"]), block_tag, words,
            ):
                return False
        self._confirm_header(header)
        selection.cursor = block
        selection.cursor_hash = block_hash
        selection.core = dict(core)
        selection.core_block = block
        selection.core_hash = block_hash
        selection.curve_state_block = block
        selection.curve_state_hash = block_hash
        selection.curve_verified_through_block = block
        selection.last_head_change = time.monotonic()
        selection.needs_seed = False
        selection.history_floor = block + 1
        selection.history_frontier_hash = None
        selection.history_complete = False
        selection.recent_history_pending = True
        selection.history_error = (
            "recent event history is loading off the live state lane"
        )
        self._sync_current_events(selection, block)
        self._record_core_tracking(selection, header, core)
        self._publish_detail(selection, self._snapshot(selection, header, core))
        selection.snapshot_dirty = False
        return True

    def _advance_selection(self, selection: _Selection) -> bool:
        header = self._head_header()
        latest = _hex_int(header["number"])
        latest_hash = str(header["hash"]).lower()
        if latest < selection.cursor:
            raise ReorgDetected(f"head rewound from {selection.cursor} to {latest}")
        if latest == selection.cursor:
            if latest_hash != selection.cursor_hash:
                raise ReorgDetected(f"same-height block {latest} was replaced")
            self._sync_current_events(selection, latest)
            core = (
                dict(selection.core)
                if selection.core is not None
                and selection.core_block == latest
                and selection.core_hash == latest_hash
                else None
            )
            refreshed_curve = False
            if selection.pool.kind in {"v3", "v4"} and core is not None:
                tick = int(core["tick"])
                wanted_words = self._viewport_words(selection, tick)
                curve_dirty = (
                    selection.curve_dirty_from_block is not None
                    and selection.curve_dirty_from_block <= latest
                )
                if wanted_words != selection.curve_words or curve_dirty:
                    if not self._load_curve_state(
                        selection, tick, hex(latest),
                    ):
                        return False
                    self._confirm_header(header)
                    selection.curve_state_block = latest
                    selection.curve_state_hash = latest_hash
                    selection.curve_verified_through_block = latest
                    selection.curve_dirty_from_block = None
                    refreshed_curve = True
            if (
                core is not None
                and (selection.snapshot_dirty or refreshed_curve)
            ):
                self._publish_detail(
                    selection, self._snapshot(selection, header, core),
                )
                selection.snapshot_dirty = False
            if time.monotonic() - selection.last_head_change > 3.0:
                self._mark_selection_error(
                    selection, "stale", "RPC head has not advanced for 3s",
                )
            return True
        if latest - selection.cursor > MAX_CATCHUP_BLOCKS:
            raise ReorgDetected(
                f"selected cursor fell {latest - selection.cursor} blocks behind; "
                "bounded reseed required"
            )
        old_header = self.rpc.call(
            "eth_getBlockByNumber", [hex(selection.cursor), False],
        )
        if (
            not isinstance(old_header, dict)
            or str(old_header.get("hash") or "").lower() != selection.cursor_hash
        ):
            raise ReorgDetected(
                f"canonical ancestry changed at block {selection.cursor}"
            )
        block_tag = hex(latest)
        core = self._read_core(selection, block_tag, seed=False)
        if self._selection_superseded(selection):
            return False
        self._sync_current_events(selection, latest)
        refreshed_curve = False
        if selection.pool.kind in {"v3", "v4"}:
            wanted_words = self._viewport_words(selection, int(core["tick"]))
            curve_dirty = (
                selection.curve_dirty_from_block is not None
                and selection.curve_dirty_from_block <= latest
            )
            if wanted_words != selection.curve_words or curve_dirty:
                if not self._load_curve_state(
                    selection, int(core["tick"]), block_tag,
                ):
                    return False
                refreshed_curve = True
        self._confirm_header(header)
        selection.cursor = latest
        selection.cursor_hash = latest_hash
        selection.core = dict(core)
        selection.core_block = latest
        selection.core_hash = latest_hash
        if selection.pool.kind == "v2":
            selection.curve_state_block = latest
            selection.curve_state_hash = latest_hash
            selection.curve_verified_through_block = latest
        if refreshed_curve:
            selection.curve_state_block = latest
            selection.curve_state_hash = latest_hash
            selection.curve_verified_through_block = latest
            selection.curve_dirty_from_block = None
        selection.last_head_change = time.monotonic()
        selection.recent_history_pending = True
        if selection.history_error is None:
            selection.history_error = (
                "recent event history is catching up off the live state lane"
            )
        self._record_core_tracking(selection, header, core)
        self._publish_detail(selection, self._snapshot(selection, header, core))
        selection.snapshot_dirty = False
        return True

    def _position_rows(self, selection: _Selection, core: dict[str, Any]) -> list[dict[str, Any]]:
        owner = selection.owner
        if owner is None:
            return []
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        if selection.pool.kind == "v3":
            output = []
            sqrt = int(core["sqrt"])
            for (state_owner, lo, hi), state in sorted(selection.participant_state.items()):
                if state_owner != owner or "claim0" not in state:
                    continue
                liquidity = int(state["liquidity"])
                owed0, owed1 = int(state["owed0"]), int(state["owed1"])
                claim0, claim1 = int(state["claim0"]), int(state["claim1"])
                if not (liquidity or claim0 or claim1 or (state_owner, lo, hi) in selection.participant_accounting):
                    continue
                raw0, raw1 = principal_raw(liquidity, sqrt, lo, hi) if liquidity else (0, 0)
                output.append({
                    "id": f"{owner}:{selection.pool.id}:{lo}:{hi}",
                    "owner": owner,
                    "lo": lo,
                    "hi": hi,
                    "liquidity": str(liquidity),
                    "amount0": _format_units(raw0, decimals0),
                    "amount1": _format_units(raw1, decimals1),
                    "amount0_raw": str(raw0),
                    "amount1_raw": str(raw1),
                    "fees0": _format_units(int(state["lazy0"]), decimals0),
                    "fees1": _format_units(int(state["lazy1"]), decimals1),
                    "fees0_raw": str(state["lazy0"]),
                    "fees1_raw": str(state["lazy1"]),
                    "tokens_owed0": _format_units(owed0, decimals0),
                    "tokens_owed1": _format_units(owed1, decimals1),
                    "tokens_owed0_raw": str(owed0),
                    "tokens_owed1_raw": str(owed1),
                    "uncollected0": _format_units(claim0, decimals0),
                    "uncollected1": _format_units(claim1, decimals1),
                    "uncollected0_raw": str(claim0),
                    "uncollected1_raw": str(claim1),
                })
            return output
        if selection.pool.kind == "v2":
            total = int(core.get("total_supply") or 0)
            balance = int(core.get("owner_balance") or 0)
            if total <= 0 or balance <= 0:
                return []
            raw0 = int(core["reserve0"]) * balance // total
            raw1 = int(core["reserve1"]) * balance // total
            return [{
                "id": f"{owner}:{selection.pool.id}",
                "owner": owner,
                "lo": -MAX_TICK,
                "hi": MAX_TICK,
                "liquidity": str(balance),
                "amount0": _format_units(raw0, decimals0),
                "amount1": _format_units(raw1, decimals1),
                "amount0_raw": str(raw0),
                "amount1_raw": str(raw1),
                "fees0": None,
                "fees1": None,
            }]
        return []

    def _concentrated_curve(self, selection: _Selection, core: dict[str, Any]) -> list[dict[str, Any]]:
        if not selection.curve_words or not selection.pool.tick_spacing:
            return []
        spacing = int(selection.pool.tick_spacing)
        low = max(-MAX_TICK, (selection.curve_words[0] << 8) * spacing)
        high = min(MAX_TICK, (((selection.curve_words[-1] + 1) << 8) - 1) * spacing)
        current_tick = int(core["tick"])
        candidates = {low, high, current_tick}
        candidates.update(tick for tick in selection.tick_net if low <= tick <= high)
        for lo, hi in selection.position_state:
            if low <= lo <= high:
                candidates.add(lo)
            if low <= hi <= high:
                candidates.add(hi)
        important = {low, high, current_tick}
        for lo, hi in selection.position_state:
            if low <= lo <= high:
                important.add(lo)
            if low <= hi <= high:
                important.add(hi)
        if len(candidates) > MAX_CURVE_POINTS:
            remaining = max(0, MAX_CURVE_POINTS - len(important))
            nearby = sorted((tick for tick in candidates if tick not in important), key=lambda tick: abs(tick - current_tick))
            output_ticks = sorted(important | set(nearby[:remaining]))
        else:
            output_ticks = sorted(candidates)

        boundaries = sorted(selection.tick_net.items())
        active = int(core["liquidity"])
        for boundary, delta in boundaries:
            if low < boundary <= current_tick:
                active -= delta
        liquidity = max(0, active)
        boundary_index = 0
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        curve = []
        for tick in output_ticks:
            while boundary_index < len(boundaries) and boundaries[boundary_index][0] <= tick:
                boundary, delta = boundaries[boundary_index]
                if boundary > low:
                    liquidity += delta
                boundary_index += 1
            ours = sum(
                value[0] for (lo, hi), value in selection.position_state.items()
                if value[0] > 0 and lo <= tick < hi
            )
            curve.append({
                "tick": tick,
                "price": _tick_price(tick, decimals0, decimals1),
                "liquidity": str(max(0, liquidity)),
                "ours": str(ours),
            })
        return curve

    def _v2_curve(self, selection: _Selection, core: dict[str, Any]) -> list[dict[str, Any]]:
        reserve0, reserve1 = int(core["reserve0"]), int(core["reserve1"])
        if reserve0 <= 0 or reserve1 <= 0:
            return []
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        if decimals0 is None or decimals1 is None:
            return []
        invariant = reserve0 * reserve1
        total = int(core.get("total_supply") or 0)
        owner_balance = int(core.get("owner_balance") or 0)
        ours = math.isqrt(invariant) * owner_balance // total if total > 0 else 0
        points = []
        for numerator, denominator in ((1, 2), (2, 3), (4, 5), (9, 10), (1, 1), (10, 9), (5, 4), (3, 2), (2, 1)):
            x = max(1, reserve0 * numerator // denominator)
            y = invariant // x
            with localcontext() as context:
                context.prec = 72
                price_decimal = Decimal(y) / Decimal(x) * (Decimal(10) ** (decimals0 - decimals1))
                price = float(price_decimal)
            raw_ratio = y / x
            equivalent_tick = (
                math.floor(math.log(raw_ratio) / math.log(1.0001))
                if raw_ratio > 0
                else None
            )
            points.append({
                "tick": equivalent_tick,
                "price": price if math.isfinite(price) else None,
                "liquidity": str(math.isqrt(invariant)),
                "ours": str(ours),
                "reserve0_raw": str(x),
                "reserve1_raw": str(y),
                "coordinate": "equivalent constant-product log-price tick",
            })
        return points

    def _capabilities(self, selection: _Selection) -> dict[str, Any]:
        if (
            selection.pool.kind == "v3"
            and selection.factory == UNISWAP_V3_FACTORY
            and selection.factory_member
        ):
            return {"simulate": True, "add": True, "remove": True, "collect": True, "why": None}
        if selection.pool.kind == "v3":
            if selection.factory != UNISWAP_V3_FACTORY:
                why = "pool factory is not the verified Uniswap V3 factory"
            else:
                why = selection.factory_membership_error or (
                    "canonical factory getPool does not return this pool"
                )
        elif selection.pool.kind == "v4":
            why = "generic V4 PositionManager ownership and hook execution are not supported safely"
        else:
            why = "V2 pair-specific fee and LP action adapter are not verified"
        return {"simulate": False, "add": False, "remove": False, "collect": False, "why": why}
    def _basket_value_usd(
        self,
        selection: _Selection,
        raw0: int,
        raw1: int,
        price_token1_per_token0: float | None,
    ) -> float | None:
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        if decimals0 is None or decimals1 is None or price_token1_per_token0 is None:
            return None
        units0 = Decimal(raw0) / (Decimal(10) ** decimals0)
        units1 = Decimal(raw1) / (Decimal(10) ** decimals1)
        quoted_price = Decimal(str(price_token1_per_token0))
        if selection.pool.token0 == USDG and selection.pool.token1 != USDG:
            value = units0 + units1 / quoted_price
        elif selection.pool.token1 == USDG and selection.pool.token0 != USDG:
            value = units1 + units0 * quoted_price
        else:
            return None
        output = float(value)
        return output if math.isfinite(output) else None

    def _participant_rows(
        self,
        selection: _Selection,
        core: dict[str, Any],
        price: float | None,
    ) -> list[dict[str, Any]]:
        if selection.pool.kind != "v3":
            return []
        metadata0 = self.universe.tokens.get(selection.pool.token0)
        metadata1 = self.universe.tokens.get(selection.pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        sqrt = int(core["sqrt"])
        tick = int(core["tick"])
        active = int(core["liquidity"])
        rows: list[dict[str, Any]] = []
        for (owner, lower, upper), state in sorted(selection.participant_state.items()):
            if "claim0" not in state:
                continue
            liquidity = int(state["liquidity"])
            claim0, claim1 = int(state["claim0"]), int(state["claim1"])
            principal0, principal1 = (
                principal_raw(liquidity, sqrt, lower, upper)
                if liquidity
                else (0, 0)
            )
            accounting = selection.participant_accounting.get((owner, lower, upper))
            fees_earned_usd: float | None = None
            fees_earned0: str | None = None
            fees_earned1: str | None = None
            pnl_usd: float | None = None
            since_block: int | None = None
            since_timestamp: int | None = None
            accounting_status = "baseline_pending"
            useful_closed = False
            if accounting is not None and accounting.get("status") == "valid":
                fee0, fee1, net0, net1 = _interval_amounts(
                    accounting, principal0, principal1, claim0, claim1
                )
                if fee0 < 0 or fee1 < 0:
                    accounting_status = "invalid_cashflow_reconciliation"
                else:
                    fees_earned0 = (
                        _format_units(fee0, decimals0)
                        if decimals0 is not None
                        else None
                    )
                    fees_earned1 = (
                        _format_units(fee1, decimals1)
                        if decimals1 is not None
                        else None
                    )
                    fees_earned_usd = self._basket_value_usd(
                        selection, fee0, fee1, price
                    )
                    equity_usd = self._basket_value_usd(
                        selection, principal0 + claim0, principal1 + claim1, price
                    )
                    baseline_usd = accounting.get("baseline_equity_usd")
                    deposited_usd = accounting.get("deposited_usd")
                    collected_usd = accounting.get("collected_usd")
                    if all(value is not None for value in (
                        equity_usd, baseline_usd, deposited_usd, collected_usd,
                    )):
                        pnl_usd = math.fsum((
                            equity_usd, collected_usd, -deposited_usd, -baseline_usd,
                        ))
                    since_block = int(accounting["baseline_block"])
                    since_timestamp = int(accounting["baseline_timestamp"])
                    accounting_status = "valid_since_observation"
                    useful_closed = any((
                        int(accounting["deposited0"]),
                        int(accounting["deposited1"]),
                        int(accounting["burn_principal0"]),
                        int(accounting["burn_principal1"]),
                        int(accounting["collected0"]),
                        int(accounting["collected1"]),
                        fee0,
                        fee1,
                        net0,
                        net1,
                    ))
            if not (liquidity or claim0 or claim1 or useful_closed):
                continue
            in_range = lower <= tick < upper
            active_share = (
                float(Decimal(liquidity) * Decimal(100) / Decimal(active))
                if in_range and active > 0
                else 0.0
            )
            value_usd = self._basket_value_usd(
                selection,
                principal0 + claim0,
                principal1 + claim1,
                price,
            )
            uncollected_usd = self._basket_value_usd(
                selection, claim0, claim1, price
            )
            rows.append({
                "id": f"{owner}:{selection.pool.id}:{lower}:{upper}",
                "owner": owner,
                "lo": lower,
                "hi": upper,
                "liquidity": str(liquidity),
                "amount0": (
                    _format_units(principal0, decimals0)
                    if decimals0 is not None
                    else None
                ),
                "amount1": (
                    _format_units(principal1, decimals1)
                    if decimals1 is not None
                    else None
                ),
                "value_usd": round(value_usd, 8) if value_usd is not None else None,
                "active_share_pct": round(active_share, 8),
                "ours": selection.owner == owner,
                "ownership_kind": selection.participant_kinds.get(
                    owner, "ownership_unresolved"
                ),
                "fees_earned_usd": (
                    round(fees_earned_usd, 8)
                    if fees_earned_usd is not None
                    else None
                ),
                "fees_earned0": fees_earned0,
                "fees_earned1": fees_earned1,
                "uncollected_usd": (
                    round(uncollected_usd, 8)
                    if uncollected_usd is not None
                    else None
                ),
                "uncollected0": (
                    _format_units(claim0, decimals0)
                    if decimals0 is not None
                    else None
                ),
                "uncollected1": (
                    _format_units(claim1, decimals1)
                    if decimals1 is not None
                    else None
                ),
                "pnl_usd": round(pnl_usd, 8) if pnl_usd is not None else None,
                "pnl_since_block": since_block,
                "pnl_since_timestamp": since_timestamp,
                "accounting_status": accounting_status,
            })
        return rows

    @staticmethod
    def _participant_coverage(
        selection: _Selection,
        participants: list[dict[str, Any]],
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        if selection.pool.kind != "v3":
            unsupported = {
                "supported": False,
                "state": "unsupported",
                "from_block": None,
                "through_block": selection.cursor or None,
            }
            return unsupported, {
                **unsupported,
                "basis": None,
                "gas_included": False,
            }, unsupported
        valid = [
            row for row in participants
            if row["accounting_status"] == "valid_since_observation"
        ]
        participant_coverage = {
            "supported": True,
            "state": (
                "error"
                if _history_error_is_failure(selection.history_error)
                or selection.participant_refresh_error
                else (
                    "truncated"
                    if selection.participant_ranges_truncated
                    else ("complete" if selection.history_complete else "indexing")
                )
            ),
            "from_block": selection.history_floor,
            "frontier_hash": selection.history_frontier_hash,
            "through_block": selection.cursor or None,
            "discovered_ranges": len(selection.participant_ranges),
            "resolved_ranges": sum(
                "claim0" in state
                for state in selection.participant_state.values()
            ),
            "visible_ranges": len(participants),
            "owners": len({
                owner for owner, _lower, _upper in selection.participant_ranges
            }),
            "ranges_truncated": selection.participant_ranges_truncated,
            "refresh_block": selection.participant_refresh_block,
            "error": selection.history_error or selection.participant_refresh_error,
            "discovery": (
                "all-owner Mint/Burn/Collect lifecycle logs; older ranges are "
                "published only when positions() remains nonempty"
            ),
        }
        since_blocks = [
            int(row["pnl_since_block"])
            for row in valid
            if row["pnl_since_block"] is not None
        ]
        since_timestamps = [
            int(row["pnl_since_timestamp"])
            for row in valid
            if row["pnl_since_timestamp"] is not None
        ]
        accounting_coverage = {
            "state": (
                "error"
                if selection.participant_refresh_error
                else ("live_interval" if valid else "baseline_pending")
            ),
            "basis": (
                "marked LP equity plus collections minus deposits minus starting "
                "equity; fee earnings subtract burned principal; before gas"
            ),
            "claimable_semantics": (
                "uncollected fields are total chain claim including lazy fees and "
                "may include removed principal; they are not labelled fee earnings"
            ),
            "since_block": min(since_blocks) if since_blocks else None,
            "since_timestamp": min(since_timestamps) if since_timestamps else None,
            "through_block": selection.cursor or None,
            "valid_positions": len(valid),
            "pending_positions": max(
                0, len(selection.participant_ranges) - len(valid)
            ),
            "gas_included": False,
            "reset_reason": selection.accounting_reset_reason,
            "continuity": (
                "verified from each per-position observation baseline; older profit is not claimed"
            ),
        }
        event_coverage = {
            "supported": True,
            "state": (
                "backfill_error"
                if _history_error_is_failure(selection.history_error)
                else (
                    "retention_truncated"
                    if selection.lp_events_truncated
                    else "bounded_live_plus_lifecycle_backfill"
                )
            ),
            "from_block": selection.lp_event_coverage_start_block,
            "from_timestamp": selection.lp_event_coverage_start_timestamp,
            "through_block": selection.cursor or None,
            "retained": len(selection.lp_events),
            "retention_truncated": selection.lp_events_truncated,
            "history_floor": selection.history_floor,
            "lifecycle_history_complete": selection.history_complete,
            "live_swap_from_block": selection.swap_coverage_start_block,
            "scope": (
                "all-owner lifecycle events are backfilled; swap events begin at "
                "the bounded live seed window"
            ),
            "error": selection.history_error,
        }
        return participant_coverage, accounting_coverage, event_coverage

    def _lp_summary(
        self,
        selection: _Selection,
        core: dict[str, Any],
        positions: list[dict[str, Any]],
        tick: int | None,
        pool_active_liquidity: int,
        price: float | None,
        header_timestamp: int,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        pool = selection.pool
        metadata0 = self.universe.tokens.get(pool.token0)
        metadata1 = self.universe.tokens.get(pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        minute_cutoff = header_timestamp - 60
        start_timestamp = selection.swap_coverage_start_timestamp
        truncated_at = selection.swaps_truncated_through_timestamp
        exact_through_head = (
            selection.swap_coverage_through_block is not None
            and selection.swap_coverage_through_block >= selection.cursor
            and (
                selection.swap_coverage_through_block > selection.cursor
                or selection.swap_coverage_through_hash
                == selection.cursor_hash
            )
        )
        minute_complete = (
            selection.history_error is None
            and exact_through_head
            and start_timestamp is not None
            and start_timestamp <= minute_cutoff
            and (truncated_at is None or truncated_at < minute_cutoff)
        )
        recent = [
            row
            for row in selection.swaps
            if int(row["timestamp"]) >= minute_cutoff
        ]
        observed_recent = [
            row
            for row in recent
            if row.get("qualification") == "verified_current_observation"
        ]
        metric_rows = recent if minute_complete else observed_recent
        metric_available = minute_complete or bool(metric_rows)

        stable_index: int | None
        stable_decimals: int | None
        if pool.token0 == USDG and pool.token1 != USDG:
            stable_index, stable_decimals = 0, decimals0
        elif pool.token1 == USDG and pool.token0 != USDG:
            stable_index, stable_decimals = 1, decimals1
        else:
            stable_index, stable_decimals = None, None

        volume: Decimal | None = None
        pool_fees: Decimal | None = None
        fee_values: list[Decimal] = []
        if (
            metric_available
            and stable_index is not None
            and stable_decimals is not None
        ):
            scale = Decimal(10) ** stable_decimals
            stable_amounts = [
                Decimal(abs(int(row[f"amount{stable_index}_raw"]))) / scale
                for row in metric_rows
            ]
            volume = sum(stable_amounts, Decimal(0))
            if not metric_rows:
                pool_fees = Decimal(0)
            else:
                for row, notional in zip(metric_rows, stable_amounts):
                    fee_ppm = row.get("fee_ppm")
                    if fee_ppm is None or not 0 <= int(fee_ppm) < 1_000_000:
                        fee_values = []
                        break
                    fee = Decimal(int(fee_ppm))
                    stable_raw = int(row[f"amount{stable_index}_raw"])
                    stable_is_input = (
                        stable_raw < 0
                        if pool.kind == "v4" else stable_raw > 0
                    )
                    denominator = (
                        Decimal(1_000_000)
                        if stable_is_input
                        else Decimal(1_000_000 - int(fee_ppm))
                    )
                    fee_values.append(notional * fee / denominator)
                if len(fee_values) == len(metric_rows):
                    pool_fees = sum(fee_values, Decimal(0))

        if pool.kind == "v3":
            our_active = sum(
                liquidity
                for (lo, hi), (liquidity, _owed0, _owed1)
                in selection.position_state.items()
                if liquidity > 0 and tick is not None and lo <= tick < hi
            )
            in_range = sum(
                1
                for (lo, hi), (liquidity, _owed0, _owed1)
                in selection.position_state.items()
                if liquidity > 0 and tick is not None and lo <= tick < hi
            )
            allocation_available = (
                selection.owner is not None and selection.position_error is None
            )
        elif pool.kind == "v2":
            total = int(core.get("total_supply") or 0)
            balance = int(core.get("owner_balance") or 0)
            our_active = (
                pool_active_liquidity * balance // total
                if total > 0 and balance > 0
                else 0
            )
            in_range = len(positions)
            allocation_available = selection.owner is not None
        else:
            our_active = 0
            in_range = 0
            allocation_available = False

        amount0_raw = sum(int(row["amount0_raw"]) for row in positions)
        amount1_raw = sum(int(row["amount1_raw"]) for row in positions)
        if allocation_available and (
            positions or pool.kind == "v2" or selection.history_complete
        ):
            amount0 = (
                _format_units(amount0_raw, decimals0)
                if decimals0 is not None
                else None
            )
            amount1 = (
                _format_units(amount1_raw, decimals1)
                if decimals1 is not None
                else None
            )
        else:
            amount0 = None
            amount1 = None

        value_usd: float | None = None
        if (
            amount0 is not None
            and amount1 is not None
            and decimals0 is not None
            and decimals1 is not None
            and price is not None
            and stable_index is not None
        ):
            units0 = Decimal(amount0_raw) / (Decimal(10) ** decimals0)
            units1 = Decimal(amount1_raw) / (Decimal(10) ** decimals1)
            quoted_price = Decimal(str(price))
            value = (
                units0 + units1 / quoted_price
                if stable_index == 0
                else units1 + units0 * quoted_price
            )
            value_usd = float(value)

        active_share = (
            float(Decimal(our_active) * Decimal(100) / Decimal(pool_active_liquidity))
            if allocation_available and pool_active_liquidity > 0
            else None
        )
        our_fees: Decimal | None = None
        recent_owner_change = any(
            int(row["timestamp"]) >= minute_cutoff
            for row in selection.owner_liquidity_changes
        )
        if (
            minute_complete
            and not recent
            and stable_index is not None
            and stable_decimals is not None
        ):
            our_fees = Decimal(0)
        elif (
            pool.kind == "v3"
            and selection.owner is not None
            and selection.history_complete
            and not selection.ranges_truncated
            and selection.position_error is None
            and not recent_owner_change
            and pool_fees is not None
            and len(fee_values) == len(recent)
        ):
            estimates: list[Decimal] = []
            for row, fee_value in zip(recent, fee_values):
                event_tick = row.get("tick")
                event_active = row.get("active_liquidity")
                if event_tick is None or event_active is None or int(event_active) <= 0:
                    estimates = []
                    break
                event_ours = sum(
                    liquidity
                    for (lo, hi), (liquidity, _owed0, _owed1)
                    in selection.position_state.items()
                    if liquidity > 0 and lo <= int(event_tick) < hi
                )
                estimates.append(
                    fee_value * Decimal(event_ours) / Decimal(int(event_active))
                )
            if len(estimates) == len(recent):
                our_fees = sum(estimates, Decimal(0))

        lp = {
            "our_active_liquidity": str(our_active),
            "active_share_pct": active_share,
            "amount0": amount0,
            "amount1": amount1,
            "value_usd": value_usd,
            "in_range": in_range,
            "position_count": len(positions),
            "swaps_1m": len(metric_rows) if metric_available else None,
            "volume_1m_usd": float(volume) if volume is not None else None,
            "pool_fees_1m_usd": (
                float(pool_fees) if pool_fees is not None else None
            ),
            "our_fees_1m_usd": float(our_fees) if our_fees is not None else None,
        }
        coverage = {
            "swaps_1m": {
                "complete": minute_complete,
                "qualification": (
                    "canonical_exact_logs"
                    if minute_complete
                    else (
                        "verified_current_observations_lower_bound"
                        if observed_recent else "unavailable"
                    )
                ),
                "from_block": selection.swap_coverage_start_block,
                "from_timestamp": start_timestamp,
                "through_block": selection.swap_coverage_through_block,
                "through_hash": selection.swap_coverage_through_hash,
                "through_timestamp": header_timestamp,
                "covered_span_s": (
                    max(0, header_timestamp - start_timestamp)
                    if start_timestamp is not None
                    else None
                ),
                "observed_current_from_block": selection.current_event_start_block,
                "observed_current_from_timestamp": (
                    selection.current_event_start_timestamp
                ),
                "observed_current_through_block": (
                    selection.current_event_through_block
                ),
                "observed_current_through_hash": (
                    selection.current_event_through_hash
                ),
                "observed_current_swaps": len(observed_recent),
                "retention_truncated_through_timestamp": truncated_at,
            },
            "positions": {
                "supported": pool.kind in {"v2", "v3"},
                "complete": (
                    selection.history_complete and not selection.ranges_truncated
                    if selection.owner and pool.kind == "v3"
                    else selection.owner is not None and pool.kind == "v2"
                ),
                "from_block": (
                    selection.history_floor
                    if selection.owner and pool.kind == "v3"
                    else None
                ),
                "frontier_hash": (
                    selection.history_frontier_hash
                    if selection.owner and pool.kind == "v3"
                    else None
                ),
                "through_block": (
                    selection.cursor
                    if selection.owner and pool.kind in {"v2", "v3"}
                    else None
                ),
                "ranges_truncated": selection.ranges_truncated,
                "error": selection.history_error,
            },
        }
        return lp, coverage

    def _snapshot(self, selection: _Selection, header: dict[str, Any], core: dict[str, Any]) -> dict[str, Any]:
        pool = selection.pool
        block = _hex_int(header["number"])
        metadata0 = self.universe.tokens.get(pool.token0)
        metadata1 = self.universe.tokens.get(pool.token1)
        decimals0 = metadata0.decimals if metadata0 else None
        decimals1 = metadata1.decimals if metadata1 else None
        header_timestamp = _hex_int(header.get("timestamp"))
        retention_cutoff = header_timestamp - 3600
        while selection.swaps and int(selection.swaps[0]["timestamp"]) < retention_cutoff:
            selection.swaps.popleft()
        while (
            selection.owner_liquidity_changes
            and int(selection.owner_liquidity_changes[0]["timestamp"]) < retention_cutoff
        ):
            selection.owner_liquidity_changes.popleft()

        health_state = "live"
        health_errors: list[str] = []
        if pool.kind == "v2":
            reserve0, reserve1 = int(core["reserve0"]), int(core["reserve1"])
            if reserve0 and reserve1:
                raw_ratio_x192 = (reserve1 << 192) // reserve0
                sqrt = math.isqrt(raw_ratio_x192)
                raw_ratio = reserve1 / reserve0
                tick = math.floor(math.log(raw_ratio) / math.log(1.0001))
                if decimals0 is not None and decimals1 is not None:
                    with localcontext() as context:
                        context.prec = 72
                        price = float(
                            Decimal(reserve1) / Decimal(reserve0)
                            * (Decimal(10) ** (decimals0 - decimals1))
                        )
                else:
                    price = None
                active_int = math.isqrt(reserve0 * reserve1)
            else:
                sqrt, tick, price, active_int = 0, None, None, 0
                health_state = "inactive"
                health_errors.append("V2 reserves are empty at the selected block")
            curve = self._v2_curve(selection, core)
        else:
            sqrt = int(core["sqrt"])
            tick = int(core["tick"])
            price = _price_from_sqrt(sqrt, decimals0, decimals1)
            active_int = int(core["liquidity"])
            curve = self._concentrated_curve(selection, core)
            if sqrt == 0:
                health_state = "inactive"
                health_errors.append(
                    "concentrated pool is uninitialized at the selected block"
                )

        missing_metadata = [
            address
            for address, metadata in (
                (pool.token0, metadata0),
                (pool.token1, metadata1),
            )
            if metadata is None or metadata.symbol is None or metadata.decimals is None
        ]
        if missing_metadata:
            if health_state == "live":
                health_state = "partial"
            health_errors.append(
                "token symbol/decimals unavailable for " + ", ".join(missing_metadata)
            )
        if selection.ranges_truncated:
            if health_state != "inactive":
                health_state = "partial"
            health_errors.append(
                f"owned-range history exceeds the bounded {MAX_POSITION_RANGES}-range cache"
            )
        if selection.participant_ranges_truncated:
            if health_state != "inactive":
                health_state = "partial"
            health_errors.append(
                "participant history exceeds the bounded "
                f"{MAX_PARTICIPANT_RANGES}-range cache"
            )
        if selection.position_error:
            if health_state == "live":
                health_state = "partial"
            health_errors.append(selection.position_error)
        if (
            _history_error_is_failure(selection.history_error)
            and selection.history_error not in health_errors
        ):
            if health_state == "live":
                health_state = "partial"
            health_errors.append(selection.history_error)
        if (
            selection.participant_refresh_error
            and selection.participant_refresh_error != selection.position_error
        ):
            if health_state == "live":
                health_state = "partial"
            health_errors.append(selection.participant_refresh_error)
        health_error = "; ".join(health_errors) if health_errors else None

        positions = self._position_rows(selection, core)
        lp, coverage = self._lp_summary(
            selection,
            core,
            positions,
            tick,
            active_int,
            price,
            header_timestamp,
        )
        participants = self._participant_rows(selection, core, price)
        participant_coverage, accounting_coverage, event_coverage = (
            self._participant_coverage(selection, participants)
        )
        coverage["participants"] = participant_coverage
        coverage["accounting"] = accounting_coverage
        coverage["lp_events"] = event_coverage
        lp_events = [
            {
                key: value
                for key, value in event.items()
                if key not in {"_order", "_current_observation"}
            }
            for event in reversed(selection.lp_events)
        ]
        block_hash = str(header["hash"]).lower()
        if pool.kind == "v2":
            curve_complete = True
            curve_state = "block_pinned"
        else:
            curve_complete = (
                bool(selection.curve_words)
                and selection.curve_dirty_from_block is None
                and selection.curve_verified_through_block is not None
                and selection.curve_verified_through_block >= block
            )
            curve_state = (
                "block_pinned"
                if curve_complete and selection.curve_state_block == block
                else "verified_unchanged"
                if curve_complete
                else "stale_topology"
                if selection.curve_words
                else "unavailable"
            )
        curve_coverage = {
            "state": curve_state,
            "state_block": selection.curve_state_block,
            "state_hash": selection.curve_state_hash,
            "verified_through_block": selection.curve_verified_through_block,
            "complete_through_snapshot": curve_complete,
        }
        coverage["curve"] = curve_coverage
        published_at = time.time()
        with self._lock:
            board = dict(self._board)
        row = self._catalog_row(pool, board, published_at)
        if selection.swaps:
            latest_swap = selection.swaps[-1]
            row["last_swap_at"] = int(latest_swap["timestamp"])
        row["block"] = block
        row["state"] = health_state
        if pool.kind == "v4" and selection.current_fee is not None:
            row["fee_ppm"] = selection.current_fee
        return {
            "pool": row,
            "block": block,
            "block_hash": block_hash,
            "block_timestamp": header_timestamp,
            "as_of": published_at,
            "freshness": {
                "snapshot_block": block,
                "snapshot_hash": block_hash,
                "snapshot_timestamp": header_timestamp,
                "current_events": {
                    "from_block": selection.current_event_start_block,
                    "from_timestamp": selection.current_event_start_timestamp,
                    "through_block": selection.current_event_through_block,
                    "through_hash": selection.current_event_through_hash,
                    "complete_through_snapshot": False,
                    "qualification": (
                        "verified_observed_events_not_exhaustive"
                        if selection.current_event_through_block is not None
                        else "unavailable"
                    ),
                    "error": selection.current_event_error,
                },
                "curve": curve_coverage,
            },
            "health": {
                "state": health_state,
                "error": health_error,
                "reorgs": selection.reorgs,
                "last_reorg": selection.last_reorg,
                "refresh_failures": selection.refresh_failures,
                "reconnects": selection.reconnects,
                "position_history_floor": (
                    selection.history_floor
                    if selection.owner and pool.kind == "v3"
                    else None
                ),
                "position_history_complete": (
                    selection.history_complete
                    if selection.owner and pool.kind == "v3"
                    else None
                ),
                "position_history_error": selection.history_error,
            },
            "providers": self.source_status(),
            "coverage": coverage,
            "spot": {
                "tick": tick,
                "sqrt_price_x96": str(sqrt),
                "price_token1_per_token0": price,
                "price_usd": _usd_price(pool.token0, pool.token1, price),
                "source": (
                    "getReserves-derived constant-product spot"
                    if pool.kind == "v2"
                    else ("StateView" if pool.kind == "v4" else "slot0")
                ),
            },
            "liquidity": {
                "active": str(active_int),
                "curve": curve,
                "model": "constant-product reserves" if pool.kind == "v2" else "initialized ticks",
                "curve_block": selection.curve_state_block,
                "curve_verified_through_block": (
                    selection.curve_verified_through_block
                ),
                "curve_complete": curve_complete,
            },
            "lp": lp,
            "tracking": list(selection.tracking),
            "positions": positions,
            "swaps": list(reversed(selection.swaps)),
            "participants": participants,
            "lp_events": lp_events,
            "capabilities": self._capabilities(selection),
        }

    def _mark_selection_error(
        self, selection: _Selection, state: str, error: str,
    ) -> None:
        counters = {
            "reorgs": selection.reorgs,
            "last_reorg": selection.last_reorg,
            "refresh_failures": selection.refresh_failures,
            "reconnects": selection.reconnects,
        }
        if selection.snapshot is None:
            snapshot = self._error_detail(selection, error, state)
            snapshot["as_of"] = time.time()
            self._publish_detail(selection, snapshot)
            return
        prior_health = selection.snapshot.get("health") or {}
        if (
            prior_health.get("state") == state
            and prior_health.get("error") == error
            and all(prior_health.get(key) == value for key, value in counters.items())
        ):
            return
        snapshot = deepcopy(selection.snapshot)
        snapshot["health"] = {
            **prior_health,
            "state": state,
            "error": error,
            **counters,
        }
        snapshot["pool"] = {**snapshot["pool"], "state": state}
        snapshot["as_of"] = time.time()
        self._publish_detail(selection, snapshot)

    def _error_detail(self, selection: _Selection, error: str, state: str = "error") -> dict[str, Any]:
        with self._lock:
            board = dict(self._board)
        published_at = (
            time.time() if selection.snapshot_revision else selection.created_at
        )
        row = self._catalog_row(selection.pool, board, published_at)
        row["state"] = state
        return {
            "pool": row,
            "revision": selection.snapshot_revision,
            "block": 0,
            "block_hash": None,
            "block_timestamp": None,
            "as_of": published_at,
            "freshness": {
                "snapshot_block": 0,
                "snapshot_hash": None,
                "snapshot_timestamp": None,
                "current_events": {
                    "from_block": None,
                    "from_timestamp": None,
                    "through_block": None,
                    "through_hash": None,
                    "complete_through_snapshot": False,
                    "qualification": "unavailable",
                    "error": error,
                },
                "curve": {
                    "state": "unavailable",
                    "state_block": None,
                    "state_hash": None,
                    "verified_through_block": None,
                    "complete_through_snapshot": False,
                },
            },
            "health": {
                "state": state,
                "error": error,
                "reorgs": selection.reorgs,
                "last_reorg": selection.last_reorg,
                "refresh_failures": selection.refresh_failures,
                "reconnects": selection.reconnects,
            },
            "providers": self.source_status(),
            "coverage": {
                "swaps_1m": {
                    "complete": False,
                    "qualification": "unavailable",
                    "from_block": None,
                    "from_timestamp": None,
                    "through_block": None,
                    "through_hash": None,
                    "through_timestamp": None,
                    "covered_span_s": None,
                    "observed_current_from_block": None,
                    "observed_current_from_timestamp": None,
                    "observed_current_through_block": None,
                    "observed_current_through_hash": None,
                    "observed_current_swaps": 0,
                    "retention_truncated_through_timestamp": None,
                },
                "positions": {
                    "supported": selection.pool.kind in {"v2", "v3"},
                    "complete": False,
                    "from_block": None,
                    "frontier_hash": None,
                    "through_block": None,
                    "ranges_truncated": False,
                    "error": None,
                },
                "participants": {
                    "supported": selection.pool.kind == "v3",
                    "state": "warming" if selection.pool.kind == "v3" else "unsupported",
                    "from_block": None,
                    "frontier_hash": None,
                    "through_block": None,
                    "discovered_ranges": 0,
                    "resolved_ranges": 0,
                    "visible_ranges": 0,
                    "owners": 0,
                    "ranges_truncated": False,
                    "refresh_block": None,
                    "error": error,
                    "discovery": (
                        "all-owner Mint/Burn/Collect lifecycle logs; older ranges "
                        "publish only when positions() remains nonempty"
                    ),
                },
                "accounting": {
                    "state": "baseline_pending" if selection.pool.kind == "v3" else "unsupported",
                    "basis": (
                        "marked LP equity plus collections minus deposits minus "
                        "starting equity; fee earnings subtract burned principal; before gas"
                    ),
                    "claimable_semantics": (
                        "uncollected fields are total chain claim including lazy fees "
                        "and may include removed principal"
                    ),
                    "since_block": None,
                    "since_timestamp": None,
                    "through_block": None,
                    "valid_positions": 0,
                    "pending_positions": 0,
                    "gas_included": False,
                    "reset_reason": selection.accounting_reset_reason,
                    "continuity": (
                        "verified from each per-position observation baseline; "
                        "older profit is not claimed"
                    ),
                },
                "lp_events": {
                    "supported": selection.pool.kind == "v3",
                    "state": "warming" if selection.pool.kind == "v3" else "unsupported",
                    "from_block": None,
                    "from_timestamp": None,
                    "through_block": None,
                    "retained": 0,
                    "retention_truncated": False,
                    "history_floor": None,
                    "lifecycle_history_complete": False,
                    "live_swap_from_block": None,
                    "scope": (
                        "all-owner lifecycle events are backfilled; swap events "
                        "begin at the bounded live seed window"
                    ),
                    "error": error,
                },
                "curve": {
                    "state": "unavailable",
                    "state_block": None,
                    "state_hash": None,
                    "verified_through_block": None,
                    "complete_through_snapshot": False,
                },
            },
            "spot": {"tick": None, "sqrt_price_x96": "0", "price_token1_per_token0": None, "price_usd": None},
            "liquidity": {
                "active": "0",
                "curve": [],
                "curve_block": None,
                "curve_verified_through_block": None,
                "curve_complete": False,
            },
            "lp": {
                "our_active_liquidity": "0",
                "active_share_pct": None,
                "amount0": None,
                "amount1": None,
                "value_usd": None,
                "in_range": 0,
                "position_count": 0,
                "swaps_1m": None,
                "volume_1m_usd": None,
                "pool_fees_1m_usd": None,
                "our_fees_1m_usd": None,
            },
            "tracking": [],
            "positions": [],
            "swaps": [],
            "participants": [],
            "lp_events": [],
            "capabilities": {"simulate": False, "add": False, "remove": False, "collect": False, "why": error},
        }
