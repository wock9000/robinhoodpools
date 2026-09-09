"""Pure LP-market ABI, trace, gas, identity, and pinned-state adapters.

All accounting values are raw-token decimal strings.  Addresses and hashes are
lower-case.  No helper in this module performs I/O; the indexer owns RPC.
"""
from __future__ import annotations

from collections import OrderedDict, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from functools import lru_cache
from threading import Lock
from types import MappingProxyType
from typing import Any
import re

from eth_hash.auto import keccak as _raw_keccak
from eth_utils import keccak

from .lp_chain import (
    POOL_MANAGER, SLIPSTREAM_FACTORY, STATE_VIEW, V2_FACTORIES,
    V3_FACTORIES as _WORKBENCH_V3_FACTORIES,
)


ZERO_ADDRESS = "0x" + "00" * 20
V4_POSITION_MANAGER = "0x58daec3116aae6d93017baaea7749052e8a04fa7"
UNISWAP_V3_POSITION_MANAGER = "0x73991a25c818bf1f1128deaab1492d45638de0d3"
PANCAKE_V3_POSITION_MANAGER = "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364"
GIGA_V3_POSITION_MANAGER = "0xa79f5775b0b49e51202c48ddf03f380faa96f641"
UNISWAP_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
PANCAKE_V3_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"
EXTENDED_V3_FACTORY = "0xece6ecd61177336ea6fb9b17937ac439d85ee20b"
_MISSPELLED_PANCAKE_V3_FACTORY = "0x0fbfcf9fa4f9c56b0f40a671ad40e0805a091865"
V3_FACTORIES = frozenset(
    {
        PANCAKE_V3_FACTORY,
        EXTENDED_V3_FACTORY,
        *(
            factory
            for factory in _WORKBENCH_V3_FACTORIES
            if factory != _MISSPELLED_PANCAKE_V3_FACTORY
        ),
    }
)
CONCENTRATED_FACTORIES = frozenset({SLIPSTREAM_FACTORY, *V3_FACTORIES})
_UINT32_FEE_PROTOCOL_FACTORIES = frozenset(
    {PANCAKE_V3_FACTORY, EXTENDED_V3_FACTORY}
)

# The two Uniswap deployments are pinned in Uniswap/contracts deployments/4663.md
# at commit 56928a9.  The Pancake deployment is additionally verified on chain by
# ERC-721/metadata interfaces and factory()==PANCAKE_V3_FACTORY.  Giga's manager
# and its extended-fee factory have explorer-verified source plus the same
# block-pinned interface and factory evidence.  The V4 entry is verified by
# poolManager()==POOL_MANAGER.  Keeping the evidence beside the allowlist makes
# an address addition a security-relevant, reviewable change.
_MANAGER_INFO = {
    UNISWAP_V3_POSITION_MANAGER: {
        "protocol": "v3",
        "factory": UNISWAP_V3_FACTORY,
        "name": "Uniswap V3 Positions NFT-V1",
        "source": "https://github.com/Uniswap/contracts/blob/56928a9/deployments/4663.md",
        "verification": "official deployment registry; on-chain factory and ERC721/ERC721Metadata",
    },
    PANCAKE_V3_POSITION_MANAGER: {
        "protocol": "v3",
        "factory": PANCAKE_V3_FACTORY,
        "name": "Pancake V3 Positions NFT-V1",
        "source": "https://github.com/pancakeswap/pancake-v3-contracts/blob/main/projects/v3-periphery/contracts/NonfungiblePositionManager.sol",
        "verification": "canonical source ABI; on-chain factory, name, symbol, and ERC721/ERC721Metadata",
    },
    GIGA_V3_POSITION_MANAGER: {
        "protocol": "v3",
        "factory": EXTENDED_V3_FACTORY,
        "name": "Giga Positions",
        "source": "https://robinhoodchain.blockscout.com/address/0xa79f5775b0b49e51202c48ddf03f380faa96f641",
        "verification": "explorer-verified NFPM source; on-chain factory, name, symbol, and ERC721/ERC721Metadata",
    },
    V4_POSITION_MANAGER: {
        "protocol": "v4",
        "pool_manager": POOL_MANAGER,
        "name": "Uniswap v4 Positions NFT",
        "source": "https://github.com/Uniswap/contracts/blob/56928a9/deployments/4663.md",
        "verification": "official deployment registry; on-chain poolManager and ERC721/ERC721Metadata",
    },
}
MANAGER_INFO: Mapping[str, Mapping[str, str]] = MappingProxyType(
    {address: MappingProxyType(dict(info)) for address, info in _MANAGER_INFO.items()}
)
NFT_MANAGER_ADDRESSES = frozenset(MANAGER_INFO)
V3_NFT_MANAGER_ADDRESSES = frozenset(
    address for address, info in MANAGER_INFO.items() if info["protocol"] == "v3"
)


def _topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


def _selector(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()[:8]


# Creation.
V4_INITIALIZE_TOPIC = _topic(
    "Initialize(bytes32,address,address,uint24,int24,address,uint160,int24)"
)
V3_POOL_CREATED_TOPIC = _topic("PoolCreated(address,address,uint24,int24,address)")
V3_POOL_CREATED_COMPACT_TOPIC = _topic("PoolCreated(address,address,uint24,address)")
SLIPSTREAM_POOL_CREATED_TOPIC = _topic("PoolCreated(address,address,int24,address)")
V2_PAIR_CREATED_TOPIC = _topic("PairCreated(address,address,address,uint256)")

# V2 core.
V2_SWAP_TOPIC = _topic("Swap(address,uint256,uint256,uint256,uint256,address)")
V2_MINT_TOPIC = _topic("Mint(address,uint256,uint256)")
V2_BURN_TOPIC = _topic("Burn(address,uint256,uint256,address)")
V2_SYNC_TOPIC = _topic("Sync(uint112,uint112)")

# V3 core.  The extended Swap topic is an existing chain-4663 concentrated
# pool ABI whose last two uint128 words are exact protocol-fee amounts.
V3_INITIALIZE_TOPIC = _topic("Initialize(uint160,int24)")
V3_SWAP_TOPIC = _topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
CL_SWAP_EXTENDED_TOPIC = (
    "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
)
V3_MINT_TOPIC = _topic("Mint(address,address,int24,int24,uint128,uint256,uint256)")
V3_BURN_TOPIC = _topic("Burn(address,int24,int24,uint128,uint256,uint256)")
V3_COLLECT_TOPIC = _topic("Collect(address,address,int24,int24,uint128,uint128)")
V3_FLASH_TOPIC = _topic("Flash(address,address,uint256,uint256,uint256,uint256)")
V3_SET_FEE_PROTOCOL_TOPIC = _topic("SetFeeProtocol(uint8,uint8,uint8,uint8)")
PANCAKE_V3_SET_FEE_PROTOCOL_TOPIC = _topic(
    "SetFeeProtocol(uint32,uint32,uint32,uint32)"
)
V3_COLLECT_PROTOCOL_TOPIC = _topic("CollectProtocol(address,address,uint128,uint128)")

# V3 NFT periphery.  These are correlation evidence, not duplicate economic
# rows; the immutable core-log row is enriched with the token id.
NFT_INCREASE_LIQUIDITY_TOPIC = _topic(
    "IncreaseLiquidity(uint256,uint128,uint256,uint256)"
)
NFT_DECREASE_LIQUIDITY_TOPIC = _topic(
    "DecreaseLiquidity(uint256,uint128,uint256,uint256)"
)
NFT_COLLECT_TOPIC = _topic("Collect(uint256,address,uint256,uint256)")

# V4 core and V4 PositionManager correlation event.
V4_SWAP_TOPIC = _topic("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")
V4_MODIFY_LIQUIDITY_TOPIC = _topic(
    "ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)"
)
V4_DONATE_TOPIC = _topic("Donate(bytes32,address,uint256,uint256)")
V4_PROTOCOL_FEE_UPDATED_TOPIC = _topic("ProtocolFeeUpdated(bytes32,uint24)")
V4_PROTOCOL_FEE_CONTROLLER_UPDATED_TOPIC = _topic(
    "ProtocolFeeControllerUpdated(address)"
)
V4_MODIFY_POSITION_TOPIC = _topic(
    "ModifyPosition(bytes32,address,int24,int24,int256,bytes32)"
)

TRANSFER_TOPIC = _topic("Transfer(address,address,uint256)")

# Transfer is intentionally separate: querying it without the manager address
# allowlist would ingest every ERC-20 transfer on the chain.
NFT_EVENT_TOPICS = (TRANSFER_TOPIC,)
EVENT_TOPICS = (
    V4_INITIALIZE_TOPIC,
    V3_POOL_CREATED_TOPIC,
    V3_POOL_CREATED_COMPACT_TOPIC,
    SLIPSTREAM_POOL_CREATED_TOPIC,
    V2_PAIR_CREATED_TOPIC,
    V2_SWAP_TOPIC,
    V2_MINT_TOPIC,
    V2_BURN_TOPIC,
    V2_SYNC_TOPIC,
    V3_INITIALIZE_TOPIC,
    V3_SWAP_TOPIC,
    CL_SWAP_EXTENDED_TOPIC,
    V3_MINT_TOPIC,
    V3_BURN_TOPIC,
    V3_COLLECT_TOPIC,
    V3_FLASH_TOPIC,
    V3_SET_FEE_PROTOCOL_TOPIC,
    PANCAKE_V3_SET_FEE_PROTOCOL_TOPIC,
    V3_COLLECT_PROTOCOL_TOPIC,
    NFT_INCREASE_LIQUIDITY_TOPIC,
    NFT_DECREASE_LIQUIDITY_TOPIC,
    NFT_COLLECT_TOPIC,
    V4_SWAP_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
    V4_DONATE_TOPIC,
    V4_PROTOCOL_FEE_UPDATED_TOPIC,
    V4_PROTOCOL_FEE_CONTROLLER_UPDATED_TOPIC,
    V4_MODIFY_POSITION_TOPIC,
)

_CREATION_TOPIC_SET = frozenset(
    {
        V4_INITIALIZE_TOPIC,
        V3_POOL_CREATED_TOPIC,
        V3_POOL_CREATED_COMPACT_TOPIC,
        SLIPSTREAM_POOL_CREATED_TOPIC,
        V2_PAIR_CREATED_TOPIC,
    }
)
_V3_CREATION_TOPIC_SET = frozenset(
    {
        V3_POOL_CREATED_TOPIC,
        V3_POOL_CREATED_COMPACT_TOPIC,
        SLIPSTREAM_POOL_CREATED_TOPIC,
    }
)
_V2_CORE_TOPIC_SET = frozenset(
    {V2_SWAP_TOPIC, V2_MINT_TOPIC, V2_BURN_TOPIC, V2_SYNC_TOPIC}
)
_V3_CORE_TOPIC_SET = frozenset(
    {
        V3_INITIALIZE_TOPIC,
        V3_SWAP_TOPIC,
        CL_SWAP_EXTENDED_TOPIC,
        V3_MINT_TOPIC,
        V3_BURN_TOPIC,
        V3_COLLECT_TOPIC,
        V3_FLASH_TOPIC,
        V3_SET_FEE_PROTOCOL_TOPIC,
        PANCAKE_V3_SET_FEE_PROTOCOL_TOPIC,
        V3_COLLECT_PROTOCOL_TOPIC,
    }
)
_V4_CORE_TOPIC_SET = frozenset(
    {
        V4_SWAP_TOPIC,
        V4_MODIFY_LIQUIDITY_TOPIC,
        V4_DONATE_TOPIC,
        V4_PROTOCOL_FEE_UPDATED_TOPIC,
        V4_PROTOCOL_FEE_CONTROLLER_UPDATED_TOPIC,
    }
)
_NFPM_AUX_TOPIC_SET = frozenset(
    {
        NFT_INCREASE_LIQUIDITY_TOPIC,
        NFT_DECREASE_LIQUIDITY_TOPIC,
        NFT_COLLECT_TOPIC,
    }
)
_AUXILIARY_TOPIC_SET = frozenset({*_NFPM_AUX_TOPIC_SET, V4_MODIFY_POSITION_TOPIC})
_SUBSCRIBED_TOPIC_SET = frozenset({*EVENT_TOPICS, *NFT_EVENT_TOPICS})

MODIFY_LIQUIDITY_SELECTOR = _selector(
    "modifyLiquidity((address,address,uint24,int24,address),(int24,int24,int256,bytes32),bytes)"
)
SWAP_SELECTOR = _selector(
    "swap((address,address,uint24,int24,address),(bool,int256,uint160),bytes)"
)
POSITIONS_SELECTOR = _selector("positions(bytes32)")
NFT_POSITIONS_SELECTOR = _selector("positions(uint256)")
SLOT0_SELECTOR = _selector("slot0()")
LIQUIDITY_SELECTOR = _selector("liquidity()")
STATE_VIEW_SLOT0_SELECTOR = _selector("getSlot0(bytes32)")
STATE_VIEW_LIQUIDITY_SELECTOR = _selector("getLiquidity(bytes32)")
STATE_VIEW_POSITION_SELECTOR = _selector("getPositionInfo(bytes32,bytes32)")

_ADDRESS_RE = re.compile(r"0x[0-9a-f]{40}\Z")
_HASH_RE = re.compile(r"0x[0-9a-f]{64}\Z")
_RPC_REVERT_DATA_RE = re.compile(
    r"""["']data["']\s*:\s*["'](0x[0-9a-fA-F]*)["']"""
)


class ProtocolDecodeError(ValueError):
    """A recognized protocol record is malformed or correlation is ambiguous."""


def manager_descriptor(address: Any) -> Mapping[str, str] | None:
    """Return immutable provenance only for an allowlisted position manager."""
    normalized = _address(address, "manager", strict=False)
    return MANAGER_INFO.get(normalized) if normalized is not None else None


def nft_position_key(manager: str, token_id: int | str) -> str:
    address = _address(manager, "NFT manager")
    token = _integer(token_id, "token id")
    if token < 0 or token >= 1 << 256:
        raise ProtocolDecodeError("NFT token id is outside uint256")
    return f"nft:{address}:{token}"


def core_position_key(protocol: str, pool_id: str, core_key: str) -> str:
    """Qualify an EVM core-position hash by its globally unique pool."""
    normalized_protocol = str(protocol).lower()
    if normalized_protocol == "v3":
        normalized_pool = _address(pool_id, "V3 core position pool")
    elif normalized_protocol == "v4":
        normalized_pool = _hash(pool_id, "V4 core position pool")
    else:
        raise ProtocolDecodeError("core position protocol must be v3 or v4")
    raw_key = _hash(core_key, "core position key")
    return f"{normalized_protocol}:{normalized_pool}:{raw_key}"


def unknown_pool_candidates(
    logs: Iterable[Mapping[str, Any]], pools: Mapping[str, Mapping[str, Any]]
) -> tuple[str, ...]:
    """Return untrusted V2/V3 event emitters that still require factory proof."""
    known = _pool_map(pools)
    candidates: set[str] = set()
    pool_topics = _V2_CORE_TOPIC_SET | _V3_CORE_TOPIC_SET
    for log in logs:
        if not isinstance(log, Mapping) or log.get("removed"):
            continue
        if _topic0(log) not in pool_topics:
            continue
        emitter = _address(log.get("address"), "candidate pool emitter", strict=False)
        if emitter is not None and emitter not in known:
            candidates.add(emitter)
    return tuple(sorted(candidates))


def _integer(value: Any, name: str) -> int:
    if isinstance(value, bool):
        raise ProtocolDecodeError(f"{name} is boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value, 16) if value.startswith(("0x", "0X")) else int(value)
        except ValueError as exc:
            raise ProtocolDecodeError(f"{name} is not an integer") from exc
    raise ProtocolDecodeError(f"{name} is not an integer")


def _address(value: Any, name: str, *, strict: bool = True) -> str | None:
    if isinstance(value, bytes) and len(value) == 20:
        text = "0x" + value.hex()
    else:
        text = str(value).lower() if isinstance(value, str) else ""
    if _ADDRESS_RE.fullmatch(text):
        return text
    if strict:
        raise ProtocolDecodeError(f"{name} is not a 20-byte address")
    return None


def _hash(value: Any, name: str) -> str:
    text = str(value).lower() if isinstance(value, str) else ""
    if not _HASH_RE.fullmatch(text):
        raise ProtocolDecodeError(f"{name} is not bytes32")
    return text


def _raw_hex(value: Any, name: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if not isinstance(value, str) or not value.startswith("0x"):
        raise ProtocolDecodeError(f"{name} is not hex data")
    body = value[2:]
    if len(body) % 2 or re.fullmatch(r"[0-9a-fA-F]*", body) is None:
        raise ProtocolDecodeError(f"{name} is malformed hex data")
    return bytes.fromhex(body)


def _words(value: Any, name: str, count: int | None = None) -> list[int]:
    raw = _raw_hex(value, name)
    if len(raw) % 32:
        raise ProtocolDecodeError(f"{name} is not whole ABI words")
    words = [int.from_bytes(raw[index : index + 32], "big") for index in range(0, len(raw), 32)]
    if count is not None and len(words) != count:
        raise ProtocolDecodeError(f"{name} has {len(words)} words, expected {count}")
    return words


def _uint(word: int, bits: int, name: str) -> int:
    if word < 0 or word >= 1 << bits:
        raise ProtocolDecodeError(f"{name} is not canonical uint{bits}")
    return word


def _sint(word: int, bits: int, name: str) -> int:
    if word < 1 << (bits - 1):
        return word
    minimum_encoded = (1 << 256) - (1 << (bits - 1))
    if word < minimum_encoded or word >= 1 << 256:
        raise ProtocolDecodeError(f"{name} is not canonical int{bits}")
    return word - (1 << 256)


def _word_address(word: int, name: str) -> str:
    _uint(word, 160, name)
    return "0x" + word.to_bytes(20, "big").hex()


def _topic_address(value: Any, name: str) -> str:
    raw = _raw_hex(value, name)
    if len(raw) != 32 or any(raw[:12]):
        raise ProtocolDecodeError(f"{name} is not a canonical indexed address")
    return "0x" + raw[12:].hex()


def _topic_uint(value: Any, bits: int, name: str) -> int:
    raw = _raw_hex(value, name)
    if len(raw) != 32:
        raise ProtocolDecodeError(f"{name} is not an indexed ABI word")
    return _uint(int.from_bytes(raw, "big"), bits, name)


def _topic_sint(value: Any, bits: int, name: str) -> int:
    raw = _raw_hex(value, name)
    if len(raw) != 32:
        raise ProtocolDecodeError(f"{name} is not an indexed ABI word")
    return _sint(int.from_bytes(raw, "big"), bits, name)


def _topics(log: Mapping[str, Any]) -> list[str]:
    values = log.get("topics")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or not values:
        return []
    topics = []
    for index, value in enumerate(values):
        topics.append(_hash(value, f"topic[{index}]") if index == 0 else "0x" + _raw_hex(value, f"topic[{index}]").hex())
    return topics


def _topic0(log: Mapping[str, Any]) -> str | None:
    values = log.get("topics")
    if not isinstance(values, Sequence) or isinstance(values, (str, bytes)) or not values:
        return None
    value = values[0]
    text = str(value).lower() if isinstance(value, str) else ""
    return text if _HASH_RE.fullmatch(text) else None


def _pool_map(pools: Mapping[str, Mapping[str, Any]]) -> dict[str, Mapping[str, Any]]:
    out: dict[str, Mapping[str, Any]] = {}
    for key, pool in pools.items():
        if not isinstance(pool, Mapping):
            continue
        for candidate in (key, pool.get("id"), pool.get("address")):
            if isinstance(candidate, str):
                out[candidate.lower()] = pool
    return out


def _pool_protocol(pool: Mapping[str, Any] | None) -> str | None:
    if pool is None:
        return None
    value = pool.get("protocol", pool.get("kind"))
    return str(value).lower() if value is not None else None


def _pool_fee(pool: Mapping[str, Any] | None) -> int | None:
    if pool is None:
        return None
    value = pool.get("fee_ppm", pool.get("fee"))
    if value is None:
        return None
    fee = _integer(value, "pool fee")
    return fee if 0 <= fee < 1_000_000 else None


def _pool_hook(pool: Mapping[str, Any] | None) -> str | None:
    if pool is None or pool.get("hook") is None:
        return None
    return _address(pool.get("hook"), "pool hook", strict=False)


def _header(headers: Mapping[Any, Mapping[str, Any]], number: int) -> Mapping[str, Any]:
    candidates = (number, str(number), hex(number))
    for key in candidates:
        value = headers.get(key)
        if isinstance(value, Mapping):
            return value
    raise ProtocolDecodeError(f"header {number} is unavailable")


def _base(log: Mapping[str, Any], headers: Mapping[Any, Mapping[str, Any]]) -> dict[str, Any]:
    block_number = _integer(log.get("blockNumber"), "log blockNumber")
    tx_index = _integer(log.get("transactionIndex"), "log transactionIndex")
    log_index = _integer(log.get("logIndex"), "log logIndex")
    if min(block_number, tx_index, log_index) < 0:
        raise ProtocolDecodeError("negative log identity component")
    block_hash = _hash(log.get("blockHash"), "log blockHash")
    tx_hash = _hash(log.get("transactionHash"), "log transactionHash")
    header = _header(headers, block_number)
    header_hash = _hash(header.get("hash"), "header hash")
    if header_hash != block_hash:
        raise ProtocolDecodeError(f"log/header hash mismatch at block {block_number}")
    timestamp = _integer(header.get("timestamp"), "header timestamp")
    if timestamp < 0:
        raise ProtocolDecodeError("negative block timestamp")
    return {
        "block_number": block_number,
        "block_hash": block_hash,
        "tx_hash": tx_hash,
        "tx_index": tx_index,
        "log_index": log_index,
        "timestamp": timestamp,
        "pool_id": None,
        "protocol": None,
        "kind": None,
        "owner": None,
        "custody": None,
        "position_key": None,
        "token_id": None,
        "tick_lower": None,
        "tick_upper": None,
        "liquidity_delta": None,
        "liquidity": None,
        "sqrt_price_x96": None,
        "tick": None,
        "fee_ppm": None,
        "amount0": None,
        "amount1": None,
        "fee_amount0": None,
        "fee_amount1": None,
        "cashflow0": None,
        "cashflow1": None,
        "price0_usd": None,
        "price1_usd": None,
        "volume_usd": None,
        "fees_usd": None,
        "deposit_usd": None,
        "withdrawal_usd": None,
        "pricing_basis": None,
        "accounting_basis": None,
        "identity_basis": None,
        "data": {},
    }


def _identity(row: Mapping[str, Any]) -> tuple[str, str, int]:
    return str(row["block_hash"]), str(row["tx_hash"]), int(row["log_index"])


def _identity_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "block_hash": row["block_hash"],
        "tx_hash": row["tx_hash"],
        "log_index": row["log_index"],
    }


def _v3_position_key(owner: str, tick_lower: int, tick_upper: int) -> str:
    payload = bytes.fromhex(owner[2:])
    payload += tick_lower.to_bytes(3, "big", signed=True)
    payload += tick_upper.to_bytes(3, "big", signed=True)
    return "0x" + keccak(payload).hex()


def _v4_position_key(owner: str, tick_lower: int, tick_upper: int, salt: str) -> str:
    payload = bytes.fromhex(owner[2:])
    payload += tick_lower.to_bytes(3, "big", signed=True)
    payload += tick_upper.to_bytes(3, "big", signed=True)
    payload += bytes.fromhex(salt[2:])
    return "0x" + keccak(payload).hex()


def _v4_pool_id(token0: str, token1: str, fee: int, spacing: int, hook: str) -> str:
    words = [
        int(token0, 16),
        int(token1, 16),
        fee,
        spacing if spacing >= 0 else (1 << 256) + spacing,
        int(hook, 16),
    ]
    return "0x" + _raw_keccak(
        b"".join(word.to_bytes(32, "big") for word in words)
    ).hex()


_V4_TICK_SPACING_HINT_LIMIT = 32768
_v4_tick_spacing_hints: OrderedDict[tuple[int, str], tuple[int, ...]] = OrderedDict()
_v4_tick_spacing_hints_lock = Lock()


def _remember_v4_tick_spacing(fee: int, hooks: str, spacing: int) -> None:
    key = (fee, hooks)
    with _v4_tick_spacing_hints_lock:
        current = _v4_tick_spacing_hints.get(key, ())
        if spacing not in current:
            _v4_tick_spacing_hints[key] = (*current, spacing)
        _v4_tick_spacing_hints.move_to_end(key)
        while len(_v4_tick_spacing_hints) > _V4_TICK_SPACING_HINT_LIMIT:
            _v4_tick_spacing_hints.popitem(last=False)


def _known_v4_tick_spacings(fee: int, hooks: str) -> tuple[int, ...]:
    key = (fee, hooks)
    with _v4_tick_spacing_hints_lock:
        hints = _v4_tick_spacing_hints.get(key, ())
        if hints:
            _v4_tick_spacing_hints.move_to_end(key)
        return hints


@lru_cache(maxsize=32768)
def _resolve_v4_tick_spacing_cached(
    pool_id: str,
    currency0: str,
    currency1: str,
    fee: int,
    hooks: str,
) -> int | None:
    encoded = bytearray(
        b"".join(
            word.to_bytes(32, "big")
            for word in (
                int(currency0, 16),
                int(currency1, 16),
                fee,
                0,
                int(hooks, 16),
            )
        )
    )
    expected = bytes.fromhex(pool_id[2:])
    # Hints only reorder the exhaustive search and are learned after a full
    # PoolKey hash match; each reuse is independently verified here.
    for candidate in _known_v4_tick_spacings(fee, hooks):
        encoded[126] = candidate >> 8
        encoded[127] = candidate & 255
        if _raw_keccak(bytes(encoded)) == expected:
            return candidate
    for candidate in range(1, 32768):
        encoded[126] = candidate >> 8
        encoded[127] = candidate & 255
        if _raw_keccak(bytes(encoded)) == expected:
            _remember_v4_tick_spacing(fee, hooks, candidate)
            return candidate
    return None


def resolve_v4_tick_spacing(
    *,
    pool_id: str,
    currency0: str,
    currency1: str,
    fee: int,
    hooks: str,
    tick_spacing: int | None = None,
) -> int | None:
    """Verify a complete V4 PoolKey hash, recovering only an omitted spacing."""
    normalized_id = str(pool_id).lower()
    normalized0 = str(currency0).lower()
    normalized1 = str(currency1).lower()
    normalized_hooks = str(hooks).lower()
    if (
        not _HASH_RE.fullmatch(normalized_id)
        or not _ADDRESS_RE.fullmatch(normalized0)
        or not _ADDRESS_RE.fullmatch(normalized1)
        or not _ADDRESS_RE.fullmatch(normalized_hooks)
        or normalized0 >= normalized1
        or isinstance(fee, bool)
        or not isinstance(fee, int)
        or not 0 <= fee < 1 << 24
        or (fee > 1_000_000 and fee != 0x800000)
        or (
            tick_spacing is not None
            and (
                isinstance(tick_spacing, bool)
                or not isinstance(tick_spacing, int)
                or not 0 < tick_spacing <= 32767
            )
        )
    ):
        return None
    if tick_spacing is not None:
        if _v4_pool_id(
            normalized0,
            normalized1,
            fee,
            tick_spacing,
            normalized_hooks,
        ) != normalized_id:
            return None
        _remember_v4_tick_spacing(fee, normalized_hooks, tick_spacing)
        return tick_spacing
    return _resolve_v4_tick_spacing_cached(
        normalized_id,
        normalized0,
        normalized1,
        fee,
        normalized_hooks,
    )


def _normalize_pool_creation(
    log: Mapping[str, Any], topic0: str
) -> dict[str, Any] | None:
    if topic0 not in _CREATION_TOPIC_SET:
        return None
    emitter = _address(log.get("address"), "creation emitter", strict=False)
    if emitter is None:
        return None
    topics = _topics(log)
    block = _integer(log.get("blockNumber"), "creation block")
    if topic0 == V4_INITIALIZE_TOPIC:
        if emitter != POOL_MANAGER:
            return None
        if len(topics) != 4:
            raise ProtocolDecodeError("malformed V4 Initialize topics")
        words = _words(log.get("data"), "V4 Initialize data", 5)
        pool_id = _hash(topics[1], "V4 pool id")
        token0 = _topic_address(topics[2], "V4 currency0")
        token1 = _topic_address(topics[3], "V4 currency1")
        fee = _uint(words[0], 24, "V4 configured fee")
        spacing = _sint(words[1], 24, "V4 tick spacing")
        hook = _word_address(words[2], "V4 hook")
        sqrt_price = _uint(words[3], 160, "V4 sqrtPriceX96")
        tick = _sint(words[4], 24, "V4 tick")
        if token0 >= token1:
            raise ProtocolDecodeError("V4 currencies are not strictly ordered")
        if _v4_pool_id(token0, token1, fee, spacing, hook) != pool_id:
            raise ProtocolDecodeError("V4 Initialize PoolKey hash mismatch")
        dynamic = bool(fee & 0x800000)
        return {
            "id": pool_id,
            "protocol": "v4",
            "address": POOL_MANAGER,
            "token0": token0,
            "token1": token1,
            "symbol0": None,
            "symbol1": None,
            "decimals0": None,
            "decimals1": None,
            "fee_ppm": None if dynamic else fee,
            "tick_spacing": spacing,
            "hook": hook,
            "factory": POOL_MANAGER,
            "created_block": block,
            "source": "PoolManager.Initialize",
            "metadata_json": {"configured_fee": fee, "dynamic_fee": dynamic},
            "_sqrt_price_x96": sqrt_price,
            "_tick": tick,
        }
    if topic0 == V2_PAIR_CREATED_TOPIC:
        if emitter not in V2_FACTORIES:
            return None
        if len(topics) != 3:
            raise ProtocolDecodeError("malformed V2 PairCreated topics")
        words = _words(log.get("data"), "V2 PairCreated data", 2)
        token0 = _topic_address(topics[1], "V2 token0")
        token1 = _topic_address(topics[2], "V2 token1")
        pair = _word_address(words[0], "V2 pair")
        return {
            "id": pair,
            "protocol": "v2",
            "address": pair,
            "token0": token0,
            "token1": token1,
            "symbol0": None,
            "symbol1": None,
            "decimals0": None,
            "decimals1": None,
            "fee_ppm": None,
            "tick_spacing": None,
            "hook": None,
            "factory": emitter,
            "created_block": block,
            "source": "V2Factory.PairCreated",
            "metadata_json": {"pair_index": str(words[1]), "discovery_basis": "factory_creation_event"},
        }
    if topic0 not in _V3_CREATION_TOPIC_SET or emitter not in CONCENTRATED_FACTORIES:
        return None
    if len(topics) < 3:
        raise ProtocolDecodeError("malformed V3 PoolCreated topics")
    token0 = _topic_address(topics[1], "V3 token0")
    token1 = _topic_address(topics[2], "V3 token1")
    fee: int | None = None
    spacing: int | None = None
    if topic0 == V3_POOL_CREATED_TOPIC:
        if len(topics) != 4:
            raise ProtocolDecodeError("malformed canonical V3 PoolCreated topics")
        words = _words(log.get("data"), "V3 PoolCreated data", 2)
        fee = _topic_uint(topics[3], 24, "V3 fee")
        spacing = _sint(words[0], 24, "V3 tick spacing")
        pool_address = _word_address(words[1], "V3 pool")
    elif topic0 == V3_POOL_CREATED_COMPACT_TOPIC:
        if len(topics) == 4:
            words = _words(log.get("data"), "compact V3 PoolCreated data", 1)
            fee = _topic_uint(topics[3], 24, "compact V3 fee")
            pool_address = _word_address(words[0], "compact V3 pool")
        elif len(topics) == 3:
            words = _words(log.get("data"), "compact V3 PoolCreated data", 2)
            fee = _uint(words[0], 24, "compact V3 fee")
            pool_address = _word_address(words[1], "compact V3 pool")
        else:
            raise ProtocolDecodeError("malformed compact V3 PoolCreated topics")
    else:
        if len(topics) == 4:
            words = _words(log.get("data"), "Slipstream PoolCreated data", 1)
            spacing = _topic_sint(topics[3], 24, "Slipstream tick spacing")
            pool_address = _word_address(words[0], "Slipstream pool")
        elif len(topics) == 3:
            words = _words(log.get("data"), "Slipstream PoolCreated data", 2)
            spacing = _sint(words[0], 24, "Slipstream tick spacing")
            pool_address = _word_address(words[1], "Slipstream pool")
        else:
            raise ProtocolDecodeError("malformed Slipstream PoolCreated topics")
    return {
        "id": pool_address,
        "protocol": "v3",
        "address": pool_address,
        "token0": token0,
        "token1": token1,
        "symbol0": None,
        "symbol1": None,
        "decimals0": None,
        "decimals1": None,
        "fee_ppm": fee,
        "tick_spacing": spacing,
        "hook": None,
        "factory": emitter,
        "created_block": block,
        "source": "SlipstreamFactory.PoolCreated" if emitter == SLIPSTREAM_FACTORY else "V3Factory.PoolCreated",
        "metadata_json": {"discovery_basis": "factory_creation_event"},
    }


def _creation_row(
    log: Mapping[str, Any], headers: Mapping[Any, Mapping[str, Any]], pool: Mapping[str, Any]
) -> dict[str, Any]:
    row = _base(log, headers)
    row.update(
        pool_id=pool["id"],
        protocol=pool["protocol"],
        kind="create",
        sqrt_price_x96=str(pool["_sqrt_price_x96"]) if "_sqrt_price_x96" in pool else None,
        tick=pool.get("_tick"),
        fee_ppm=pool.get("fee_ppm"),
        pricing_basis="event_state" if pool["protocol"] == "v4" else None,
        accounting_basis="creation_event",
        identity_basis="factory_or_manager_event",
    )
    stored_pool = {key: value for key, value in pool.items() if not key.startswith("_")}
    row["data"] = {"pool": stored_pool}
    return row


def _decode_transfer(
    log: Mapping[str, Any], headers: Mapping[Any, Mapping[str, Any]]
) -> dict[str, Any] | None:
    manager = _address(log.get("address"), "Transfer emitter", strict=False)
    if manager not in NFT_MANAGER_ADDRESSES:
        return None
    topics = _topics(log)
    if len(topics) != 4:
        raise ProtocolDecodeError("malformed recognized NFT Transfer topics")
    if _raw_hex(log.get("data"), "NFT Transfer data"):
        raise ProtocolDecodeError("recognized NFT Transfer has non-empty data")
    prior_owner = _topic_address(topics[1], "NFT Transfer from")
    new_owner = _topic_address(topics[2], "NFT Transfer to")
    token_id = _topic_uint(topics[3], 256, "NFT token id")
    row = _base(log, headers)
    row.update(
        protocol="nft",
        kind="transfer",
        owner=None if new_owner == ZERO_ADDRESS else new_owner,
        custody=manager,
        position_key=nft_position_key(manager, token_id),
        token_id=str(token_id),
        accounting_basis="ownership_only_no_cashflow",
        identity_basis="recognized_manager_transfer",
    )
    row["data"] = {
        "manager_protocol": MANAGER_INFO[manager]["protocol"],
        "prior_owner": prior_owner,
        "new_owner": new_owner,
        "mint": prior_owner == ZERO_ADDRESS,
        "burn": new_owner == ZERO_ADDRESS,
        "manager_source": MANAGER_INFO[manager]["source"],
    }
    return row


def _decode_event(
    log: Mapping[str, Any],
    headers: Mapping[Any, Mapping[str, Any]],
    pools: Mapping[str, Mapping[str, Any]],
) -> dict[str, Any] | None:
    topic0 = _topic0(log)
    if topic0 is None:
        return None
    if topic0 == TRANSFER_TOPIC:
        return _decode_transfer(log, headers)
    pool_creation = _normalize_pool_creation(log, topic0)
    if pool_creation is not None:
        return _creation_row(log, headers, pool_creation)

    emitter = _address(log.get("address"), "event emitter", strict=False)
    if emitter is None:
        return None
    topics = _topics(log)
    pool = pools.get(emitter)
    protocol = _pool_protocol(pool)

    if topic0 in _V2_CORE_TOPIC_SET:
        if protocol != "v2":
            return None
        row = _base(log, headers)
        row.update(pool_id=str(pool.get("id", emitter)).lower(), protocol="v2", fee_ppm=_pool_fee(pool))
        if topic0 == V2_SWAP_TOPIC:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V2 Swap topics")
            words = _words(log.get("data"), "V2 Swap data", 4)
            values = [_uint(word, 256, f"V2 Swap word {index}") for index, word in enumerate(words)]
            amount0 = values[0] - values[2]
            amount1 = values[1] - values[3]
            row.update(kind="swap", amount0=str(amount0), amount1=str(amount1), pricing_basis="event_delta")
            row["data"] = {
                "sender": _topic_address(topics[1], "V2 Swap sender"),
                "recipient": _topic_address(topics[2], "V2 Swap recipient"),
                "input0": str(values[0]),
                "input1": str(values[1]),
                "output0": str(values[2]),
                "output1": str(values[3]),
                "swap_fee_ppm": row["fee_ppm"],
                "fees_basis": "gross_swap_fee_estimate" if row["fee_ppm"] is not None else "unknown_pair_fee",
            }
        elif topic0 == V2_MINT_TOPIC:
            if len(topics) != 2:
                raise ProtocolDecodeError("malformed V2 Mint topics")
            words = _words(log.get("data"), "V2 Mint data", 2)
            row.update(kind="add", amount0=str(words[0]), amount1=str(words[1]), accounting_basis="pool_event_beneficiary_unknown", identity_basis="unresolved_v2_lp_token")
            row["data"] = {"sender": _topic_address(topics[1], "V2 Mint sender")}
        elif topic0 == V2_BURN_TOPIC:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V2 Burn topics")
            words = _words(log.get("data"), "V2 Burn data", 2)
            row.update(kind="remove", amount0=str(words[0]), amount1=str(words[1]), accounting_basis="pool_event_beneficiary_unknown", identity_basis="unresolved_v2_lp_token")
            row["data"] = {
                "sender": _topic_address(topics[1], "V2 Burn sender"),
                "recipient": _topic_address(topics[2], "V2 Burn recipient"),
            }
        else:
            if len(topics) != 1:
                raise ProtocolDecodeError("malformed V2 Sync topics")
            words = _words(log.get("data"), "V2 Sync data", 2)
            reserve0 = _uint(words[0], 112, "V2 reserve0")
            reserve1 = _uint(words[1], 112, "V2 reserve1")
            row.update(kind="checkpoint", pricing_basis="event_reserves")
            row["data"] = {"reserve0": str(reserve0), "reserve1": str(reserve1)}
        return row

    if topic0 in _V3_CORE_TOPIC_SET:
        if protocol != "v3":
            return None
        row = _base(log, headers)
        row.update(pool_id=str(pool.get("id", emitter)).lower(), protocol="v3", fee_ppm=_pool_fee(pool))
        if topic0 == V3_INITIALIZE_TOPIC:
            if len(topics) != 1:
                raise ProtocolDecodeError("malformed V3 Initialize topics")
            words = _words(log.get("data"), "V3 Initialize data", 2)
            row.update(kind="checkpoint", sqrt_price_x96=str(_uint(words[0], 160, "V3 sqrtPriceX96")), tick=_sint(words[1], 24, "V3 tick"), pricing_basis="event_state")
        elif topic0 in {V3_SWAP_TOPIC, CL_SWAP_EXTENDED_TOPIC}:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V3 Swap topics")
            count = 7 if topic0 == CL_SWAP_EXTENDED_TOPIC else 5
            words = _words(log.get("data"), "V3 Swap data", count)
            amount0 = _sint(words[0], 256, "V3 amount0")
            amount1 = _sint(words[1], 256, "V3 amount1")
            row.update(
                kind="swap",
                amount0=str(amount0),
                amount1=str(amount1),
                sqrt_price_x96=str(_uint(words[2], 160, "V3 sqrtPriceX96")),
                liquidity=str(_uint(words[3], 128, "V3 active liquidity")),
                tick=_sint(words[4], 24, "V3 tick"),
                pricing_basis="event_state_and_delta",
            )
            data = {
                "sender": _topic_address(topics[1], "V3 Swap sender"),
                "recipient": _topic_address(topics[2], "V3 Swap recipient"),
                "input0": str(max(amount0, 0)),
                "input1": str(max(amount1, 0)),
                "output0": str(max(-amount0, 0)),
                "output1": str(max(-amount1, 0)),
                "swap_fee_ppm": row["fee_ppm"],
                "fees_basis": "gross_swap_fee_estimate" if row["fee_ppm"] is not None else "unknown_pool_fee",
            }
            if count == 7:
                protocol_fee0 = _uint(words[5], 128, "Pancake V3 protocolFeesToken0")
                protocol_fee1 = _uint(words[6], 128, "Pancake V3 protocolFeesToken1")
                data.update(
                    protocol_fee_amount0=str(protocol_fee0),
                    protocol_fee_amount1=str(protocol_fee1),
                    protocol_fee_observed=True,
                    fees_basis="protocol_fee_event_exact_gross_swap_fee_from_tier",
                )
            row["data"] = data
        elif topic0 == V3_MINT_TOPIC:
            if len(topics) != 4:
                raise ProtocolDecodeError("malformed V3 Mint topics")
            words = _words(log.get("data"), "V3 Mint data", 4)
            sender = _word_address(words[0], "V3 Mint sender")
            owner = _topic_address(topics[1], "V3 Mint owner")
            lower = _topic_sint(topics[2], 24, "V3 tickLower")
            upper = _topic_sint(topics[3], 24, "V3 tickUpper")
            liquidity = _uint(words[1], 128, "V3 Mint liquidity")
            raw_core_key = _v3_position_key(owner, lower, upper)
            row.update(
                kind="add",
                custody=owner,
                position_key=core_position_key("v3", row["pool_id"], raw_core_key),
                tick_lower=lower, tick_upper=upper, liquidity_delta=str(liquidity),
                amount0=str(words[2]), amount1=str(words[3]),
                cashflow0=str(-words[2]), cashflow1=str(-words[3]),
                accounting_basis="core_mint_exact_position_flow", identity_basis="core_position_custody",
            )
            row["data"] = {"sender": sender, "core_position_key": raw_core_key}
        elif topic0 == V3_BURN_TOPIC:
            if len(topics) != 4:
                raise ProtocolDecodeError("malformed V3 Burn topics")
            words = _words(log.get("data"), "V3 Burn data", 3)
            owner = _topic_address(topics[1], "V3 Burn owner")
            lower = _topic_sint(topics[2], 24, "V3 tickLower")
            upper = _topic_sint(topics[3], 24, "V3 tickUpper")
            liquidity = _uint(words[0], 128, "V3 Burn liquidity")
            raw_core_key = _v3_position_key(owner, lower, upper)
            row.update(
                kind="remove" if liquidity else "checkpoint",
                custody=owner,
                position_key=core_position_key(
                    "v3", row["pool_id"], raw_core_key
                ),
                tick_lower=lower,
                tick_upper=upper,
                liquidity_delta=str(-liquidity),
                amount0=str(words[1]),
                amount1=str(words[2]),
                accounting_basis=(
                    "principal_moved_to_position_claim_not_wallet_cashflow"
                    if liquidity
                    else "zero_burn_fee_checkpoint"
                ),
                identity_basis="core_position_custody",
            )
            row["data"] = {"core_position_key": raw_core_key}
        elif topic0 == V3_COLLECT_TOPIC:
            if len(topics) != 4:
                raise ProtocolDecodeError("malformed V3 Collect topics")
            words = _words(log.get("data"), "V3 Collect data", 3)
            owner = _topic_address(topics[1], "V3 Collect owner")
            lower = _topic_sint(topics[2], 24, "V3 tickLower")
            upper = _topic_sint(topics[3], 24, "V3 tickUpper")
            recipient = _word_address(words[0], "V3 Collect recipient")
            amount0 = _uint(words[1], 128, "V3 Collect amount0")
            amount1 = _uint(words[2], 128, "V3 Collect amount1")
            raw_core_key = _v3_position_key(owner, lower, upper)
            row.update(
                kind="collect",
                custody=owner,
                position_key=core_position_key(
                    "v3", row["pool_id"], raw_core_key
                ),
                tick_lower=lower, tick_upper=upper, amount0=str(amount0), amount1=str(amount1),
                cashflow0=str(amount0), cashflow1=str(amount1),
                accounting_basis="core_collect_exact_position_flow", identity_basis="core_position_custody",
            )
            row["data"] = {
                "recipient": recipient,
                "core_position_key": raw_core_key,
            }
        elif topic0 == V3_FLASH_TOPIC:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V3 Flash topics")
            words = _words(log.get("data"), "V3 Flash data", 4)
            paid0 = _uint(words[2], 256, "V3 Flash paid0")
            paid1 = _uint(words[3], 256, "V3 Flash paid1")
            row.update(
                kind="fee",
                amount0=str(_uint(words[0], 256, "V3 Flash amount0")),
                amount1=str(_uint(words[1], 256, "V3 Flash amount1")),
                fee_amount0=str(paid0),
                fee_amount1=str(paid1),
                accounting_basis="flash_paid_exact_fee_and_overpayment",
            )
            row["data"] = {
                "sender": _topic_address(topics[1], "V3 Flash sender"),
                "recipient": _topic_address(topics[2], "V3 Flash recipient"),
                "paid0": str(paid0), "paid1": str(paid1),
                "fees_basis": "gross_pool_flash_paid_exact",
            }
        elif topic0 in {
            V3_SET_FEE_PROTOCOL_TOPIC,
            PANCAKE_V3_SET_FEE_PROTOCOL_TOPIC,
        }:
            if len(topics) != 1:
                raise ProtocolDecodeError("malformed V3 SetFeeProtocol topics")
            words = _words(log.get("data"), "V3 SetFeeProtocol data", 4)
            packed_policy = topic0 == PANCAKE_V3_SET_FEE_PROTOCOL_TOPIC
            bits = 32 if packed_policy else 8
            values = [_uint(word, bits, "V3 protocol fee") for word in words]
            names = (
                (
                    "fee_protocol0_ppm_old",
                    "fee_protocol1_ppm_old",
                    "fee_protocol0_ppm_new",
                    "fee_protocol1_ppm_new",
                )
                if packed_policy
                else (
                    "fee_protocol0_divisor_old",
                    "fee_protocol1_divisor_old",
                    "fee_protocol0_divisor_new",
                    "fee_protocol1_divisor_new",
                )
            )
            row.update(kind="fee", accounting_basis="protocol_configuration")
            row["data"] = dict(zip(names, values))
            factory = _address(
                pool.get("factory"), "V3 fee-policy factory", strict=False
            )
            row["data"]["fee_protocol_encoding"] = (
                "pancake_uint16_ppm_each"
                if factory == PANCAKE_V3_FACTORY
                else "extended_v3_uint16_ppm_each"
                if packed_policy
                else "uniswap_uint4_divisor_each"
            )
        else:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V3 CollectProtocol topics")
            words = _words(log.get("data"), "V3 CollectProtocol data", 2)
            amount0 = _uint(words[0], 128, "V3 protocol amount0")
            amount1 = _uint(words[1], 128, "V3 protocol amount1")
            row.update(kind="fee", amount0=str(amount0), amount1=str(amount1), accounting_basis="protocol_fee_withdrawal_not_lp_revenue")
            row["data"] = {
                "sender": _topic_address(topics[1], "V3 protocol fee sender"),
                "recipient": _topic_address(topics[2], "V3 protocol fee recipient"),
            }
        return row

    if topic0 in _V4_CORE_TOPIC_SET:
        if emitter != POOL_MANAGER:
            return None
        row = _base(log, headers)
        row["protocol"] = "v4"
        if topic0 == V4_PROTOCOL_FEE_CONTROLLER_UPDATED_TOPIC:
            if len(topics) != 2 or _raw_hex(log.get("data"), "V4 fee controller data"):
                raise ProtocolDecodeError("malformed V4 ProtocolFeeControllerUpdated")
            row.update(kind="fee", accounting_basis="protocol_configuration")
            row["data"] = {"protocol_fee_controller": _topic_address(topics[1], "V4 fee controller")}
            return row
        if len(topics) < 2:
            raise ProtocolDecodeError("malformed V4 event topics")
        pool_id = _hash(topics[1], "V4 pool id")
        pool = pools.get(pool_id)
        row["pool_id"] = pool_id
        if topic0 == V4_SWAP_TOPIC:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V4 Swap topics")
            words = _words(log.get("data"), "V4 Swap data", 6)
            amount0 = _sint(words[0], 128, "V4 amount0")
            amount1 = _sint(words[1], 128, "V4 amount1")
            swap_fee = _uint(words[5], 24, "V4 combined swap fee")
            row.update(
                kind="swap", amount0=str(amount0), amount1=str(amount1),
                sqrt_price_x96=str(_uint(words[2], 160, "V4 sqrtPriceX96")),
                liquidity=str(_uint(words[3], 128, "V4 active liquidity")),
                tick=_sint(words[4], 24, "V4 tick"), fee_ppm=swap_fee,
                pricing_basis="event_state_and_delta",
            )
            row["data"] = {
                "sender": _topic_address(topics[2], "V4 Swap sender"),
                # V4 emits the caller BalanceDelta: debt/input is negative.
                "input0": str(max(-amount0, 0)), "input1": str(max(-amount1, 0)),
                "output0": str(max(amount0, 0)), "output1": str(max(amount1, 0)),
                "swap_fee_ppm": swap_fee,
                "fees_basis": "gross_combined_swap_fee_event",
            }
        elif topic0 == V4_MODIFY_LIQUIDITY_TOPIC:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V4 ModifyLiquidity topics")
            words = _words(log.get("data"), "V4 ModifyLiquidity data", 4)
            custody = _topic_address(topics[2], "V4 ModifyLiquidity sender")
            lower = _sint(words[0], 24, "V4 tickLower")
            upper = _sint(words[1], 24, "V4 tickUpper")
            liquidity_delta = _sint(words[2], 256, "V4 liquidityDelta")
            salt = "0x" + words[3].to_bytes(32, "big").hex()
            kind = "add" if liquidity_delta > 0 else "remove" if liquidity_delta < 0 else "collect"
            raw_core_key = _v4_position_key(custody, lower, upper, salt)
            token_id = str(words[3]) if custody == V4_POSITION_MANAGER else None
            position_key = (
                nft_position_key(custody, token_id)
                if token_id is not None
                else core_position_key("v4", pool_id, raw_core_key)
            )
            row.update(
                kind=kind, custody=custody, position_key=position_key, token_id=token_id,
                tick_lower=lower, tick_upper=upper, liquidity_delta=str(liquidity_delta),
                accounting_basis="pending_trace", identity_basis="verified_v4_nft_salt" if token_id is not None else "core_position_custody",
            )
            row["data"] = {
                "salt": salt,
                "core_position_key": raw_core_key,
                "trace_complete": False,
                "cashflow_basis": "pending_trace",
            }
            if token_id is not None:
                row["data"]["nft_position_key"] = position_key
        elif topic0 == V4_DONATE_TOPIC:
            if len(topics) != 3:
                raise ProtocolDecodeError("malformed V4 Donate topics")
            words = _words(log.get("data"), "V4 Donate data", 2)
            row.update(kind="donate", amount0=str(words[0]), amount1=str(words[1]), fee_amount0=str(words[0]), fee_amount1=str(words[1]), accounting_basis="donation_to_active_liquidity")
            row["data"] = {"sender": _topic_address(topics[2], "V4 Donate sender"), "fees_basis": "donate_event_exact_unallocated"}
        else:
            if len(topics) != 2:
                raise ProtocolDecodeError("malformed V4 ProtocolFeeUpdated topics")
            words = _words(log.get("data"), "V4 ProtocolFeeUpdated data", 1)
            packed = _uint(words[0], 24, "V4 protocol fee")
            row.update(kind="fee", accounting_basis="protocol_configuration")
            row["data"] = {
                "protocol_fee": packed,
                "protocol_fee_zero_for_one": packed & 0xFFF,
                "protocol_fee_one_for_zero": packed >> 12,
            }
        return row
    return None


def _decode_nfpm_aux(log: Mapping[str, Any]) -> dict[str, Any] | None:
    topic0 = _topic0(log)
    manager = _address(log.get("address"), "NFT event emitter", strict=False)
    if manager not in V3_NFT_MANAGER_ADDRESSES or topic0 not in _NFPM_AUX_TOPIC_SET:
        return None
    topics = _topics(log)
    if len(topics) != 2:
        raise ProtocolDecodeError("malformed recognized V3 NFT position event topics")
    token_id = _topic_uint(topics[1], 256, "V3 NFT token id")
    words = _words(log.get("data"), "V3 NFT position data", 3)
    if topic0 == NFT_COLLECT_TOPIC:
        kind = "collect"
        recipient = _word_address(words[0], "V3 NFT collect recipient")
        liquidity = None
        amount0, amount1 = words[1], words[2]
    else:
        kind = "add" if topic0 == NFT_INCREASE_LIQUIDITY_TOPIC else "remove"
        recipient = None
        liquidity = _uint(words[0], 128, "V3 NFT liquidity")
        amount0, amount1 = words[1], words[2]
    return {
        "manager": manager,
        "kind": kind,
        "token_id": token_id,
        "liquidity": liquidity,
        "amount0": amount0,
        "amount1": amount1,
        "recipient": recipient,
        "tx_hash": _hash(log.get("transactionHash"), "NFT event transaction hash"),
        "log_index": _integer(log.get("logIndex"), "NFT event log index"),
    }


def _decode_v4_position_aux(log: Mapping[str, Any]) -> dict[str, Any] | None:
    if _topic0(log) != V4_MODIFY_POSITION_TOPIC:
        return None
    manager = _address(log.get("address"), "V4 ModifyPosition emitter", strict=False)
    if manager != V4_POSITION_MANAGER:
        return None
    topics = _topics(log)
    if len(topics) != 3:
        raise ProtocolDecodeError("malformed recognized V4 ModifyPosition topics")
    words = _words(log.get("data"), "V4 ModifyPosition data", 4)
    return {
        "manager": manager,
        "pool_id": _hash(topics[1], "V4 ModifyPosition pool id"),
        "operator": _topic_address(topics[2], "V4 ModifyPosition sender"),
        "tick_lower": _sint(words[0], 24, "V4 ModifyPosition tickLower"),
        "tick_upper": _sint(words[1], 24, "V4 ModifyPosition tickUpper"),
        "liquidity_delta": _sint(words[2], 256, "V4 ModifyPosition liquidityDelta"),
        "salt": "0x" + words[3].to_bytes(32, "big").hex(),
        "tx_hash": _hash(log.get("transactionHash"), "V4 ModifyPosition transaction hash"),
        "log_index": _integer(log.get("logIndex"), "V4 ModifyPosition log index"),
    }


def _receipt_for(receipts: Mapping[str, Any] | None, tx_hash: str) -> Mapping[str, Any] | None:
    if not receipts:
        return None
    if "transactionHash" in receipts:
        candidate: Any = receipts
    else:
        candidate = receipts.get(tx_hash) or receipts.get(tx_hash.lower())
    if isinstance(candidate, Mapping) and "result" in candidate and "transactionHash" not in candidate:
        candidate = candidate.get("result")
    return candidate if isinstance(candidate, Mapping) else None


def _trace_for(traces: Mapping[str, Any] | None, tx_hash: str) -> Mapping[str, Any] | None:
    if not traces:
        return None
    if "input" in traces or "calls" in traces:
        candidate: Any = traces
    else:
        candidate = traces.get(tx_hash) or traces.get(tx_hash.lower())
    if isinstance(candidate, Mapping) and "result" in candidate and "input" not in candidate:
        candidate = candidate.get("result")
    return candidate if isinstance(candidate, Mapping) else None


def _evidence_logs(
    input_logs: Sequence[Mapping[str, Any]], receipts: Mapping[str, Any] | None
) -> dict[str, list[Mapping[str, Any]]]:
    grouped: dict[str, dict[int, Mapping[str, Any]]] = defaultdict(dict)
    for log in input_logs:
        tx_hash = str(log.get("transactionHash") or "").lower()
        if _HASH_RE.fullmatch(tx_hash):
            grouped[tx_hash][_integer(log.get("logIndex"), "log index")] = log
    for tx_hash in tuple(grouped):
        receipt = _receipt_for(receipts, tx_hash)
        if receipt is None:
            continue
        receipt_hash = _hash(receipt.get("transactionHash"), "receipt transaction hash")
        if receipt_hash != tx_hash:
            raise ProtocolDecodeError("receipt transaction hash mismatch")
        values = receipt.get("logs")
        if not isinstance(values, Sequence) or isinstance(values, (str, bytes)):
            raise ProtocolDecodeError("receipt logs are unavailable")
        for log in values:
            if not isinstance(log, Mapping):
                raise ProtocolDecodeError("receipt contains a non-object log")
            grouped[tx_hash][_integer(log.get("logIndex"), "receipt log index")] = log
    return {tx_hash: [by_index[index] for index in sorted(by_index)] for tx_hash, by_index in grouped.items()}


def _owner_from_same_tx_transfers(
    logs: Sequence[Mapping[str, Any]], manager: str, token_id: int, action_index: int
) -> tuple[str | None, str | None]:
    transfers: list[tuple[int, str, str]] = []
    for log in logs:
        if _topic0(log) != TRANSFER_TOPIC or _address(log.get("address"), "transfer emitter", strict=False) != manager:
            continue
        topics = _topics(log)
        if len(topics) != 4:
            raise ProtocolDecodeError("malformed recognized NFT Transfer topics")
        if _topic_uint(topics[3], 256, "NFT token id") != token_id:
            continue
        transfers.append((
            _integer(log.get("logIndex"), "transfer log index"),
            _topic_address(topics[1], "NFT transfer from"),
            _topic_address(topics[2], "NFT transfer to"),
        ))
    if not transfers:
        return None, None
    transfers.sort()
    before = [item for item in transfers if item[0] <= action_index]
    if before:
        _, prior, new = before[-1]
        owner = new if new != ZERO_ADDRESS else prior
        return owner, "manager_transfer_same_tx"
    _, prior, new = transfers[0]
    if prior == ZERO_ADDRESS:
        return new, "manager_mint_transfer_same_tx"
    return prior, "manager_transfer_same_tx_prestate"


def repair_v4_owners(
    events: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Return V4 manager core rows whose owner is proven by same-tx transfers.

    The input is not mutated.  Returned rows retain all canonical enrichment
    fields and differ only in owner attribution and its evidence basis.
    """
    rows: list[Mapping[str, Any]] = []
    transfers: dict[
        tuple[str, int], dict[int, tuple[str, str]]
    ] = defaultdict(dict)
    for row in events:
        if not isinstance(row, Mapping):
            raise TypeError("normalized transaction events must be mappings")
        rows.append(row)
        custody = _address(row.get("custody"), "normalized event custody", strict=False)
        if (
            row.get("protocol") != "nft"
            or row.get("kind") != "transfer"
            or custody != V4_POSITION_MANAGER
        ):
            continue
        token_id = _integer(row.get("token_id"), "V4 transfer token id")
        if (
            str(row.get("position_key") or "").lower()
            != nft_position_key(V4_POSITION_MANAGER, token_id)
        ):
            raise ProtocolDecodeError(
                "V4 manager transfer has inconsistent NFT position key"
            )
        data = row.get("data")
        if not isinstance(data, Mapping):
            raise ProtocolDecodeError("V4 manager transfer data is missing")
        prior_owner = _address(data.get("prior_owner"), "V4 transfer prior owner")
        new_owner = _address(data.get("new_owner"), "V4 transfer new owner")
        if prior_owner == ZERO_ADDRESS and new_owner == ZERO_ADDRESS:
            raise ProtocolDecodeError("V4 manager transfer has two zero endpoints")
        identity = (
            _hash(row.get("tx_hash"), "V4 transfer transaction"),
            token_id,
        )
        log_index = _integer(row.get("log_index"), "V4 transfer log index")
        existing = transfers[identity].get(log_index)
        endpoints = (prior_owner, new_owner)
        if existing is not None and existing != endpoints:
            raise ProtocolDecodeError("conflicting normalized V4 manager transfers")
        transfers[identity][log_index] = endpoints

    changed: list[dict[str, Any]] = []
    for row in rows:
        custody = _address(row.get("custody"), "normalized event custody", strict=False)
        if (
            row.get("protocol") != "v4"
            or custody != V4_POSITION_MANAGER
            or row.get("liquidity_delta") is None
        ):
            continue
        token_id = _integer(row.get("token_id"), "V4 core token id")
        if (
            str(row.get("position_key") or "").lower()
            != nft_position_key(V4_POSITION_MANAGER, token_id)
        ):
            raise ProtocolDecodeError(
                "V4 manager core row has inconsistent NFT position key"
            )
        tx_hash = _hash(row.get("tx_hash"), "V4 core transaction")
        candidates = sorted(
            (index, *owners)
            for index, owners in transfers.get((tx_hash, token_id), {}).items()
        )
        if not candidates:
            continue
        action_index = _integer(row.get("log_index"), "V4 core log index")
        before = [item for item in candidates if item[0] <= action_index]
        if before:
            _index, prior_owner, new_owner = before[-1]
            owner = new_owner if new_owner != ZERO_ADDRESS else prior_owner
            basis = "manager_transfer_same_tx"
        else:
            _index, prior_owner, new_owner = candidates[0]
            if prior_owner == ZERO_ADDRESS:
                owner = new_owner
                basis = "manager_mint_transfer_same_tx"
            else:
                owner = prior_owner
                basis = "manager_transfer_same_tx_prestate"
        if owner == ZERO_ADDRESS:
            raise ProtocolDecodeError("V4 manager transfer cannot prove a zero owner")
        data = row.get("data")
        if not isinstance(data, Mapping):
            raise ProtocolDecodeError("V4 manager core data is missing")
        if (
            row.get("owner") == owner
            and row.get("identity_basis") == basis
            and data.get("identity_basis") == basis
        ):
            continue
        repaired = dict(row)
        repaired_data = dict(data)
        repaired.update(owner=owner, identity_basis=basis, data=repaired_data)
        repaired_data["identity_basis"] = basis
        changed.append(repaired)
    return changed


def _correlate_manager_events(
    rows: list[dict[str, Any]],
    evidence: Mapping[str, Sequence[Mapping[str, Any]]],
    headers: Mapping[Any, Mapping[str, Any]],
) -> None:
    by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_tx[row["tx_hash"]].append(row)
    for tx_hash, tx_rows in by_tx.items():
        logs = evidence.get(tx_hash, ())
        nfpm_aux = [item for log in logs if (item := _decode_nfpm_aux(log)) is not None]
        v4_aux = [item for log in logs if (item := _decode_v4_position_aux(log)) is not None]

        core_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in tx_rows:
            if row["protocol"] != "v3" or row["custody"] not in V3_NFT_MANAGER_ADDRESSES or row["kind"] not in {"add", "remove", "collect"}:
                continue
            key = (
                row["custody"], row["kind"],
                abs(int(row["liquidity_delta"])) if row["kind"] != "collect" else None,
                int(row["amount0"]), int(row["amount1"]),
            )
            core_groups[key].append(row)
        aux_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for aux in nfpm_aux:
            key = (aux["manager"], aux["kind"], aux["liquidity"], aux["amount0"], aux["amount1"])
            aux_groups[key].append(aux)
        for key, auxiliaries in aux_groups.items():
            cores = sorted(core_groups.get(key, ()), key=lambda row: row["log_index"])
            auxiliaries.sort(key=lambda item: item["log_index"])
            if not cores:
                # A manager-only receipt can be decoded later after its pool is discovered;
                # never fabricate a pool or duplicate the manager event meanwhile.
                continue
            if len(cores) != len(auxiliaries):
                # Mint's indexed owner is caller-supplied.  A permissionless
                # direct pool mint can therefore imitate NFPM custody and
                # collide with a legitimate manager action.  Preserve every
                # core cashflow but decline NFT attribution for the whole key.
                for row in cores:
                    row["data"].update(
                        nft_correlation_status="ambiguous",
                        nft_core_candidate_count=len(cores),
                        nft_aux_candidate_count=len(auxiliaries),
                    )
                continue
            for row, aux in zip(cores, auxiliaries):
                token_id = aux["token_id"]
                core_key = _hash(
                    row["data"].get("core_position_key"),
                    "V3 core position key",
                )
                row["token_id"] = str(token_id)
                row["position_key"] = nft_position_key(aux["manager"], token_id)
                owner, basis = _owner_from_same_tx_transfers(logs, aux["manager"], token_id, row["log_index"])
                row["owner"] = owner
                row["identity_basis"] = basis or "verified_nft_token_core_correlation"
                row["data"].update(
                    core_position_key=core_key,
                    nft_manager=aux["manager"],
                    nft_event_log_index=aux["log_index"],
                    nft_position_key=row["position_key"],
                    identity_basis=row["identity_basis"],
                )
                if aux["recipient"] is not None:
                    row["data"]["nft_recipient"] = aux["recipient"]

        v4_core_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for row in tx_rows:
            if row["protocol"] != "v4" or row["custody"] != V4_POSITION_MANAGER or row["liquidity_delta"] is None:
                continue
            key = (
                row["pool_id"], row["tick_lower"], row["tick_upper"],
                int(row["liquidity_delta"]), row["data"]["salt"],
            )
            v4_core_groups[key].append(row)
        if v4_core_groups:
            repair_input: list[Mapping[str, Any]] = list(tx_rows)
            known = {_identity(row) for row in tx_rows}
            for log in logs:
                if (
                    _topic0(log) != TRANSFER_TOPIC
                    or _address(
                        log.get("address"), "transfer emitter", strict=False
                    ) != V4_POSITION_MANAGER
                ):
                    continue
                transfer = _decode_transfer(log, headers)
                if transfer is not None and _identity(transfer) not in known:
                    repair_input.append(transfer)
            repairs = {
                _identity(row): row for row in repair_v4_owners(repair_input)
            }
            for row in tx_rows:
                repaired = repairs.get(_identity(row))
                if repaired is None:
                    continue
                row["owner"] = repaired["owner"]
                row["identity_basis"] = repaired["identity_basis"]
                row["data"] = repaired["data"]
            for cores in v4_core_groups.values():
                for row in cores:
                    row["data"].setdefault("identity_basis", row["identity_basis"])
        v4_aux_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
        for aux in v4_aux:
            key = (aux["pool_id"], aux["tick_lower"], aux["tick_upper"], aux["liquidity_delta"], aux["salt"])
            v4_aux_groups[key].append(aux)
        for key, auxiliaries in v4_aux_groups.items():
            cores = sorted(v4_core_groups.get(key, ()), key=lambda row: row["log_index"])
            auxiliaries.sort(key=lambda item: item["log_index"])
            if not cores:
                continue
            if len(cores) != len(auxiliaries):
                raise ProtocolDecodeError("ambiguous V4 PositionManager/core event correlation")
            for row, aux in zip(cores, auxiliaries):
                row["data"].update(
                    position_manager_operator=aux["operator"],
                    position_manager_event_log_index=aux["log_index"],
                )

        # Enrich Transfer rows when a same-transaction core action resolved the
        # token's pool/range, while retaining the Transfer log's own identity.
        token_rows = {
            (row["custody"], row["token_id"]): row
            for row in tx_rows
            if row["token_id"] is not None and row["protocol"] in {"v3", "v4"}
        }
        for transfer in tx_rows:
            if transfer["protocol"] != "nft":
                continue
            action = token_rows.get((transfer["custody"], transfer["token_id"]))
            if action is None:
                continue
            transfer.update(
                pool_id=action["pool_id"], tick_lower=action["tick_lower"],
                tick_upper=action["tick_upper"],
            )
            transfer["data"].update(
                action_position_key=action["position_key"],
                core_position_key=action["data"].get("core_position_key"),
            )


def _trace_log_key(log: Mapping[str, Any]) -> tuple[str, tuple[str, ...], str]:
    address = _address(log.get("address"), "trace log address")
    topics = tuple(_topics(log))
    data = "0x" + _raw_hex(log.get("data"), "trace log data").hex()
    return address, topics, data


def _decode_balance_delta(word: int) -> tuple[int, int]:
    high = (word >> 128) & ((1 << 128) - 1)
    low = word & ((1 << 128) - 1)
    if high >= 1 << 127:
        high -= 1 << 128
    if low >= 1 << 127:
        low -= 1 << 128
    return high, low


def _decode_modify_call(frame: Mapping[str, Any]) -> dict[str, Any]:
    raw = _raw_hex(frame.get("input"), "modifyLiquidity trace input")
    if len(raw) < 4 or "0x" + raw[:4].hex() != MODIFY_LIQUIDITY_SELECTOR:
        raise ProtocolDecodeError("trace frame is not canonical modifyLiquidity")
    args = raw[4:]
    if len(args) < 11 * 32 or len(args) % 32:
        raise ProtocolDecodeError("modifyLiquidity calldata is truncated")
    words = [int.from_bytes(args[index : index + 32], "big") for index in range(0, len(args), 32)]
    token0 = _word_address(words[0], "modifyLiquidity currency0")
    token1 = _word_address(words[1], "modifyLiquidity currency1")
    fee = _uint(words[2], 24, "modifyLiquidity fee")
    spacing = _sint(words[3], 24, "modifyLiquidity tick spacing")
    hook = _word_address(words[4], "modifyLiquidity hook")
    lower = _sint(words[5], 24, "modifyLiquidity tickLower")
    upper = _sint(words[6], 24, "modifyLiquidity tickUpper")
    liquidity_delta = _sint(words[7], 256, "modifyLiquidity liquidityDelta")
    salt = "0x" + words[8].to_bytes(32, "big").hex()
    offset = words[9]
    if offset != 10 * 32:
        raise ProtocolDecodeError("modifyLiquidity hookData has non-canonical offset")
    size = words[10]
    padded = (size + 31) // 32 * 32
    if len(args) != offset + 32 + padded:
        raise ProtocolDecodeError("modifyLiquidity hookData length is inconsistent")
    if any(args[offset + 32 + size :]):
        raise ProtocolDecodeError("modifyLiquidity hookData padding is non-zero")
    output = _raw_hex(frame.get("output"), "modifyLiquidity trace output")
    if len(output) != 64:
        raise ProtocolDecodeError("modifyLiquidity must return exactly two BalanceDelta words")
    caller_word = int.from_bytes(output[:32], "big")
    fees_word = int.from_bytes(output[32:], "big")
    caller0, caller1 = _decode_balance_delta(caller_word)
    fees0, fees1 = _decode_balance_delta(fees_word)
    return {
        "pool_id": _v4_pool_id(token0, token1, fee, spacing, hook),
        "token0": token0, "token1": token1, "configured_fee": fee,
        "tick_spacing": spacing, "hook": hook,
        "tick_lower": lower, "tick_upper": upper,
        "liquidity_delta": liquidity_delta, "salt": salt,
        "caller_delta0": caller0, "caller_delta1": caller1,
        "fees_accrued0": fees0, "fees_accrued1": fees1,
    }


def _walk_trace(
    frame: Mapping[str, Any], path: tuple[int, ...] = (), failed_ancestor: bool = False
) -> Iterable[tuple[tuple[int, ...], Mapping[str, Any], bool]]:
    failed = failed_ancestor or bool(frame.get("error") or frame.get("revertReason"))
    yield path, frame, failed
    calls = frame.get("calls") or ()
    if not isinstance(calls, Sequence) or isinstance(calls, (str, bytes)):
        raise ProtocolDecodeError("trace calls is not a sequence")
    for index, child in enumerate(calls):
        if not isinstance(child, Mapping):
            raise ProtocolDecodeError("trace contains a non-object call")
        yield from _walk_trace(child, path + (index,), failed)


def _trace_updates(
    receipt: Mapping[str, Any], trace: Mapping[str, Any]
) -> dict[tuple[str, str, int], dict[str, Any]]:
    status = _integer(receipt.get("status"), "receipt status")
    if status != 1:
        return {}
    receipt_hash = _hash(receipt.get("transactionHash"), "receipt transaction hash")
    block_hash = _hash(receipt.get("blockHash"), "receipt block hash")
    receipt_logs = receipt.get("logs")
    if not isinstance(receipt_logs, Sequence) or isinstance(receipt_logs, (str, bytes)):
        raise ProtocolDecodeError("receipt logs are unavailable for trace correlation")
    matching_receipt: dict[tuple[str, tuple[str, ...], str], list[Mapping[str, Any]]] = defaultdict(list)
    for log in receipt_logs:
        if isinstance(log, Mapping) and _topic0(log) == V4_MODIFY_LIQUIDITY_TOPIC and _address(log.get("address"), "receipt log emitter", strict=False) == POOL_MANAGER:
            matching_receipt[_trace_log_key(log)].append(log)

    calls_by_key: dict[tuple[str, tuple[str, ...], str], list[tuple[tuple[int, ...], dict[str, Any]]]] = defaultdict(list)
    for path, frame, failed in _walk_trace(trace):
        selector = str(frame.get("input") or "")[:10].lower()
        if selector != MODIFY_LIQUIDITY_SELECTOR:
            continue
        if _address(frame.get("to"), "trace call target", strict=False) != POOL_MANAGER:
            continue
        if failed:
            continue
        if str(frame.get("type") or "CALL").upper() != "CALL":
            raise ProtocolDecodeError("modifyLiquidity trace frame is not CALL")
        decoded = _decode_modify_call(frame)
        frame_logs = frame.get("logs") or ()
        if not isinstance(frame_logs, Sequence) or isinstance(frame_logs, (str, bytes)):
            raise ProtocolDecodeError("modifyLiquidity trace logs is not a sequence")
        candidates = [
            log for log in frame_logs
            if isinstance(log, Mapping)
            and _topic0(log) == V4_MODIFY_LIQUIDITY_TOPIC
            and _address(log.get("address"), "trace log emitter", strict=False) == POOL_MANAGER
        ]
        if len(candidates) != 1:
            raise ProtocolDecodeError("successful modifyLiquidity call did not emit exactly one core event")
        event_log = candidates[0]
        topics = _topics(event_log)
        words = _words(event_log.get("data"), "traced ModifyLiquidity event", 4)
        if len(topics) != 3:
            raise ProtocolDecodeError("traced ModifyLiquidity event topics are malformed")
        event_values = (
            _hash(topics[1], "traced pool id"),
            _topic_address(topics[2], "traced sender"),
            _sint(words[0], 24, "traced tickLower"),
            _sint(words[1], 24, "traced tickUpper"),
            _sint(words[2], 256, "traced liquidityDelta"),
            "0x" + words[3].to_bytes(32, "big").hex(),
        )
        call_from = _address(frame.get("from"), "modifyLiquidity caller")
        call_values = (
            decoded["pool_id"], call_from, decoded["tick_lower"], decoded["tick_upper"],
            decoded["liquidity_delta"], decoded["salt"],
        )
        if event_values != call_values:
            raise ProtocolDecodeError("modifyLiquidity trace call/event key mismatch")
        calls_by_key[_trace_log_key(event_log)].append((path, decoded))

    if not matching_receipt and not calls_by_key:
        return {}
    if not calls_by_key and matching_receipt:
        raise ProtocolDecodeError("trace omitted successful receipt modifyLiquidity calls")
    updates: dict[tuple[str, str, int], dict[str, Any]] = {}
    for key, calls in calls_by_key.items():
        receipt_group = matching_receipt.get(key, ())
        if len(calls) != len(receipt_group):
            raise ProtocolDecodeError("ambiguous trace/receipt ModifyLiquidity correlation")
        paths = [item[0] for item in calls]
        for index, left in enumerate(paths):
            for right in paths[index + 1 :]:
                if (
                    left == right[: len(left)]
                    or right == left[: len(right)]
                ):
                    # Parent LOG placement relative to a nested call is not
                    # represented by lexicographic frame paths, so identical
                    # event bytes cannot identify their distinct return deltas.
                    raise ProtocolDecodeError(
                        "ambiguous nested duplicate ModifyLiquidity trace key"
                    )
        calls.sort(key=lambda item: item[0])
        receipt_group = sorted(
            receipt_group,
            key=lambda log: _integer(log.get("logIndex"), "receipt log index"),
        )
        for (_, decoded), log in zip(calls, receipt_group):
            log_tx_hash = _hash(log.get("transactionHash"), "receipt log transaction hash")
            log_block_hash = _hash(log.get("blockHash"), "receipt log block hash")
            if log_tx_hash != receipt_hash or log_block_hash != block_hash:
                raise ProtocolDecodeError("trace/receipt log identity mismatch")
            identity = (block_hash, receipt_hash, _integer(log.get("logIndex"), "receipt log index"))
            updates[identity] = decoded
    unmatched = set(matching_receipt) - set(calls_by_key)
    if unmatched:
        raise ProtocolDecodeError("receipt contains uncorrelated successful ModifyLiquidity event")
    return updates


def _enrich_v4_traces(
    rows: list[dict[str, Any]],
    receipts: Mapping[str, Any] | None,
    traces: Mapping[str, Any] | None,
    pools: Mapping[str, Mapping[str, Any]],
) -> None:
    by_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        if row["protocol"] == "v4" and row["liquidity_delta"] is not None:
            by_tx[row["tx_hash"]].append(row)
    for tx_hash, tx_rows in by_tx.items():
        receipt = _receipt_for(receipts, tx_hash)
        trace = _trace_for(traces, tx_hash)
        if receipt is None or trace is None:
            continue
        updates = _trace_updates(receipt, trace)
        for row in tx_rows:
            decoded = updates.get(_identity(row))
            if decoded is None:
                continue
            expected = (
                row["pool_id"], row["custody"], row["tick_lower"], row["tick_upper"],
                int(row["liquidity_delta"]), row["data"]["salt"],
            )
            actual = (
                decoded["pool_id"],
                # The event sender was already checked against trace frame.from.
                row["custody"], decoded["tick_lower"], decoded["tick_upper"],
                decoded["liquidity_delta"], decoded["salt"],
            )
            if expected != actual:
                raise ProtocolDecodeError("immutable V4 row does not match trace enrichment")
            caller0, caller1 = decoded["caller_delta0"], decoded["caller_delta1"]
            fees0, fees1 = decoded["fees_accrued0"], decoded["fees_accrued1"]
            row.update(
                cashflow0=str(caller0), cashflow1=str(caller1),
                fee_amount0=str(fees0), fee_amount1=str(fees1),
                accounting_basis="v4_modifyLiquidity_return",
            )
            hook = decoded["hook"]
            row["data"].update(
                trace_complete=True,
                caller_delta={"amount0": str(caller0), "amount1": str(caller1)},
                fees_accrued={"amount0": str(fees0), "amount1": str(fees1)},
                fees_accrued_exact=True,
                fees_basis="modifyLiquidity_return_exact_but_donate_inflatable",
                input0=str(max(-caller0, 0)), input1=str(max(-caller1, 0)),
                output0=str(max(caller0, 0)), output1=str(max(caller1, 0)),
                cashflow_basis="v4_modifyLiquidity_return",
                configured_fee=decoded["configured_fee"], tick_spacing=decoded["tick_spacing"],
                hook=hook,
                principal_and_hook_delta={
                    "amount0": str(caller0 - fees0), "amount1": str(caller1 - fees1)
                },
            )
            row["data"]["pool_key"] = {
                "token0": decoded["token0"],
                "token1": decoded["token1"],
                "configured_fee": decoded["configured_fee"],
                "tick_spacing": decoded["tick_spacing"],
                "hook": hook,
            }
            if pools.get(row["pool_id"]) is None:
                dynamic = bool(decoded["configured_fee"] & 0x800000)
                row["data"]["pool"] = {
                    "id": row["pool_id"],
                    "protocol": "v4",
                    "address": POOL_MANAGER,
                    "token0": decoded["token0"],
                    "token1": decoded["token1"],
                    "symbol0": None,
                    "symbol1": None,
                    "decimals0": None,
                    "decimals1": None,
                    "fee_ppm": None if dynamic else decoded["configured_fee"],
                    "tick_spacing": decoded["tick_spacing"],
                    "hook": hook,
                    "factory": POOL_MANAGER,
                    "created_block": None,
                    "source": "PoolManager.modifyLiquidity trace",
                    "metadata_json": {
                        "configured_fee": decoded["configured_fee"],
                        "dynamic_fee": dynamic,
                        "observed_block": row["block_number"],
                    },
                }
            if hook == ZERO_ADDRESS:
                row["data"]["principal_delta"] = dict(row["data"]["principal_and_hook_delta"])
                row["data"]["principal_delta_exact"] = True


def decode_logs(
    logs: list[dict[str, Any]],
    pools: dict[str, dict[str, Any]],
    headers: dict[int, dict[str, Any]],
    receipts: dict[str, dict[str, Any]] | None = None,
    traces: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Normalize supported logs, optionally enriching immutable rows.

    Unrelated emitters are skipped.  A recognized emitter/topic with malformed
    ABI raises :class:`ProtocolDecodeError`; corrupt records are never guessed.
    """
    if not isinstance(logs, list) or not isinstance(pools, Mapping) or not isinstance(headers, Mapping):
        raise TypeError("logs, pools, and headers must be JSON-shaped collections")
    subscribed_topics = _SUBSCRIBED_TOPIC_SET
    ordered_logs = sorted(
        (
            log
            for log in logs
            if isinstance(log, Mapping)
            and not log.get("removed")
            and _topic0(log) in subscribed_topics
        ),
        key=lambda log: (
            _integer(log.get("blockNumber"), "log blockNumber"),
            _integer(log.get("transactionIndex"), "log transactionIndex"),
            _integer(log.get("logIndex"), "log logIndex"),
        ),
    )
    local_pools: dict[str, Mapping[str, Any]] = _pool_map(pools)
    creations: dict[tuple[str, str, int], Mapping[str, Any]] = {}
    for log in ordered_logs:
        topic0 = _topic0(log)
        if topic0 in _CREATION_TOPIC_SET:
            pool = _normalize_pool_creation(log, topic0)
            if pool is not None:
                identity = (
                    _hash(log.get("blockHash"), "creation block hash"),
                    _hash(log.get("transactionHash"), "creation transaction hash"),
                    _integer(log.get("logIndex"), "creation log index"),
                )
                creations[identity] = pool
                local_pools[str(pool["id"]).lower()] = pool
                local_pools[str(pool["address"]).lower()] = pool

    rows: list[dict[str, Any]] = []
    auxiliary_topics = _AUXILIARY_TOPIC_SET
    for log in ordered_logs:
        topic0 = _topic0(log)
        if topic0 in auxiliary_topics:
            # Strictly validate allowlisted auxiliary events now; their values
            # enrich core rows below rather than becoming duplicate cashflows.
            if topic0 == V4_MODIFY_POSITION_TOPIC:
                _decode_v4_position_aux(log)
            else:
                _decode_nfpm_aux(log)
            continue
        identity = (
            str(log.get("blockHash") or "").lower(),
            str(log.get("transactionHash") or "").lower(),
            _integer(log.get("logIndex"), "log index"),
        )
        if identity in creations:
            row = _creation_row(log, headers, creations[identity])
        else:
            row = _decode_event(log, headers, local_pools)
        if row is not None:
            rows.append(row)

    evidence = _evidence_logs(ordered_logs, receipts)
    _correlate_manager_events(rows, evidence, headers)
    _enrich_v4_traces(rows, receipts, traces, local_pools)
    rows.sort(key=lambda row: (row["block_number"], row["tx_index"], row["log_index"]))
    return rows


def decode_gas_record(
    receipt: Mapping[str, Any], transaction: Mapping[str, Any] | None = None
) -> dict[str, Any]:
    """Normalize exact Nitro receipt gas without adding L1 gas a second time."""
    if not isinstance(receipt, Mapping):
        raise TypeError("receipt must be a mapping")
    transaction = transaction or {}
    gas_used = _integer(receipt.get("gasUsed"), "receipt gasUsed")
    gas_price_value = receipt.get("effectiveGasPrice")
    gas_price_source = "receipt.effectiveGasPrice"
    if gas_price_value is None:
        gas_price_value = transaction.get("gasPrice")
        gas_price_source = "transaction.gasPrice"
    gas_price = _integer(gas_price_value, "effective gas price")
    status = _integer(receipt.get("status"), "receipt status")
    if gas_used < 0 or gas_price < 0 or status not in {0, 1}:
        raise ProtocolDecodeError("invalid receipt gas/status")
    payer = _address(receipt.get("from"), "receipt payer", strict=False)
    if payer is None:
        payer = _address(transaction.get("from"), "transaction payer")
    data: dict[str, Any] = {
        "gas_price_source": gas_price_source,
        "gas_semantics": "nitro_gasUsed_times_effectiveGasPrice_includes_total_receipt_charge",
        "l1_gas_not_added_twice": True,
    }
    if receipt.get("gasUsedForL1") is not None:
        data["gas_used_for_l1"] = str(_integer(receipt["gasUsedForL1"], "receipt gasUsedForL1"))
    if receipt.get("l1BlockNumber") is not None:
        data["l1_block_number"] = _integer(receipt["l1BlockNumber"], "receipt l1BlockNumber")
    return {
        "tx_hash": _hash(receipt.get("transactionHash"), "receipt transaction hash"),
        "block_number": _integer(receipt.get("blockNumber"), "receipt block number"),
        "block_hash": _hash(receipt.get("blockHash"), "receipt block hash"),
        "payer": payer,
        "gas_used": str(gas_used),
        "gas_price": str(gas_price),
        "gas_native": str(gas_used * gas_price),
        "gas_usd": None,
        "status": status,
        "data": data,
    }


def _abi_word(value: int) -> str:
    return value.to_bytes(32, "big").hex()


def _call_data(selector: str, *words: int) -> str:
    return selector + "".join(_abi_word(word) for word in words)


def _state_request(
    row: Mapping[str, Any], field: str, decoder: str, to: str, data: str,
    block_number: int, **extra: Any,
) -> dict[str, Any]:
    correlation = {
        "identity": _identity_dict(row),
        "field": field,
        "decoder": decoder,
        "phase": "before" if field.endswith("before") else "after",
        "block_number": block_number,
    }
    correlation.update(extra)
    return {
        "method": "eth_call",
        "params": [{"to": to, "data": data}, hex(block_number)],
        "correlation": correlation,
    }


def position_state_requests(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Build deterministic, block-pinned position and opening pool-state calls."""
    requests: list[dict[str, Any]] = []
    seen: set[tuple[Any, ...]] = set()
    ordered_events = sorted(
        events,
        key=lambda item: (
            int(item["block_number"]),
            int(item["tx_index"]),
            int(item["log_index"]),
        ),
    )
    lifecycle_proofs: set[tuple[str, str, int, str]] = set()
    for proof_row in ordered_events:
        proof_data = (
            proof_row.get("data")
            if isinstance(proof_row.get("data"), Mapping)
            else {}
        )
        manager = _address(
            proof_row.get("custody"), "lifecycle proof manager", strict=False
        )
        token_value = proof_row.get("token_id")
        if (
            proof_row.get("protocol") != "nft"
            or proof_row.get("kind") != "transfer"
            or manager not in NFT_MANAGER_ADDRESSES
            or proof_data.get("manager_protocol") != MANAGER_INFO[manager]["protocol"]
            or token_value is None
        ):
            continue
        token_id = _integer(token_value, "lifecycle proof token id")
        if (
            str(proof_row.get("position_key") or "").lower()
            != nft_position_key(manager, token_id)
        ):
            continue
        tx_hash = _hash(proof_row.get("tx_hash"), "lifecycle proof transaction")
        prior_owner = _address(
            proof_data.get("prior_owner"), "lifecycle prior owner", strict=False
        )
        new_owner = _address(
            proof_data.get("new_owner"), "lifecycle new owner", strict=False
        )
        if (
            proof_data.get("mint") is True
            and prior_owner == ZERO_ADDRESS
            and new_owner not in {None, ZERO_ADDRESS}
        ):
            lifecycle_proofs.add((tx_hash, manager, token_id, "mint"))
        if (
            proof_data.get("burn") is True
            and new_owner == ZERO_ADDRESS
            and prior_owner not in {None, ZERO_ADDRESS}
        ):
            lifecycle_proofs.add((tx_hash, manager, token_id, "burn"))
    for row in ordered_events:
        if row.get("kind") not in {"add", "remove", "collect", "checkpoint", "transfer"}:
            continue
        block = int(row["block_number"])
        pins = (("position_before", max(block - 1, 0)), ("position_after", block))
        protocol = row.get("protocol")
        custody = _address(row.get("custody"), "position custody", strict=False)
        token_id_value = row.get("token_id")
        data = row.get("data") if isinstance(row.get("data"), Mapping) else {}
        core_key_value = data.get("core_position_key")
        if core_key_value is None:
            legacy_key = str(row.get("position_key") or "").lower()
            core_key_value = legacy_key if _HASH_RE.fullmatch(legacy_key) else None
        core_key = (
            _hash(core_key_value, "core position key")
            if core_key_value is not None
            else None
        )
        pool_value = row.get("pool") if isinstance(row.get("pool"), Mapping) else {}
        token_id: int | None = None
        proof_identity: tuple[str, str, int] | None = None
        mint_absence_basis: str | None = None
        burn_absence_basis: str | None = None
        if token_id_value is not None and custody in NFT_MANAGER_ADDRESSES:
            token_id = _integer(token_id_value, "NFT token id")
            if (
                str(row.get("position_key") or "").lower()
                == nft_position_key(custody, token_id)
            ):
                proof_identity = (
                    _hash(row.get("tx_hash"), "NFT event transaction"),
                    custody,
                    token_id,
                )
            if proof_identity is not None and (*proof_identity, "mint") in lifecycle_proofs:
                mint_absence_basis = (
                    "same_receipt_verified_nfpm_mint"
                    if custody in V3_NFT_MANAGER_ADDRESSES
                    else "same_receipt_verified_v4_manager_mint"
                )
            if (
                custody == V4_POSITION_MANAGER
                and proof_identity is not None
                and (*proof_identity, "burn") in lifecycle_proofs
            ):
                burn_absence_basis = "same_receipt_verified_v4_manager_burn"
        for field, pin in pins:
            if field == "position_before" and mint_absence_basis is not None:
                # An allowlisted manager's canonical zero-address mint proves
                # this monotonically assigned NFT position did not exist at
                # the parent block. Attach that proof to the required after
                # read instead of spending an archive call on a known revert.
                continue
            request: dict[str, Any] | None = None
            if token_id is not None and custody in V3_NFT_MANAGER_ADDRESSES:
                missing_basis = None
                if (
                    field == "position_after"
                    and proof_identity is not None
                    and (*proof_identity, "burn") in lifecycle_proofs
                ):
                    missing_basis = "same_receipt_verified_nfpm_burn"
                request = _state_request(
                    row,
                    field,
                    "v3_nfpm_position",
                    custody,
                    _call_data(NFT_POSITIONS_SELECTOR, token_id),
                    pin,
                    manager=custody,
                    token_id=str(token_id),
                    allow_missing=missing_basis is not None,
                    missing_basis=missing_basis,
                    position_before_absence_basis=(
                        mint_absence_basis if field == "position_after" else None
                    ),
                    position_before_block=max(block - 1, 0),
                    expected_tick_lower=row.get("tick_lower"),
                    expected_tick_upper=row.get("tick_upper"),
                    expected_token0=pool_value.get("token0"),
                    expected_token1=pool_value.get("token1"),
                    expected_fee_ppm=pool_value.get("fee_ppm"),
                )
            elif protocol == "v3" and core_key is not None:
                pool_address = _address(row.get("pool_id"), "V3 pool", strict=False)
                if pool_address is not None:
                    request = _state_request(
                        row,
                        field,
                        "v3_core_position",
                        pool_address,
                        POSITIONS_SELECTOR + core_key[2:].lower(),
                        pin,
                        allow_empty=field == "position_before" and row.get("kind") == "add",
                    )
            elif (
                (protocol == "v4" or custody == V4_POSITION_MANAGER)
                and isinstance(row.get("pool_id"), str)
                and core_key is not None
            ):
                pool_id = _hash(row["pool_id"], "V4 pool id")
                position_key = _hash(core_key, "V4 position key")
                request = _state_request(
                    row, field, "v4_state_view_position", STATE_VIEW,
                    STATE_VIEW_POSITION_SELECTOR + pool_id[2:] + position_key[2:], pin,
                    position_before_absence_basis=(
                        mint_absence_basis if field == "position_after" else None
                    ),
                    position_before_block=max(block - 1, 0),
                    position_after_absence_basis=(
                        burn_absence_basis if field == "position_after" else None
                    ),
                )
            if request is not None:
                key = (tuple(request["correlation"]["identity"].items()), field, request["params"][0]["to"], request["params"][0]["data"], pin)
                if key not in seen:
                    seen.add(key)
                    requests.append(request)

        if (
            row.get("kind") not in {"add", "remove", "collect", "checkpoint"}
            or protocol not in {"v3", "v4"}
            or row.get("position_key") is None
        ):
            continue
        pin = max(block - 1, 0)
        if protocol == "v3":
            target = _address(row.get("pool_id"), "V3 pool", strict=False)
            components = (
                ("v3_pool_slot0", target, SLOT0_SELECTOR),
                ("v3_pool_liquidity", target, LIQUIDITY_SELECTOR),
            )
        else:
            pool_id = _hash(row.get("pool_id"), "V4 pool id")
            components = (
                ("v4_pool_slot0", STATE_VIEW, STATE_VIEW_SLOT0_SELECTOR + pool_id[2:]),
                ("v4_pool_liquidity", STATE_VIEW, STATE_VIEW_LIQUIDITY_SELECTOR + pool_id[2:]),
            )
        for decoder, target, calldata in components:
            if target is None:
                continue
            request = _state_request(
                row,
                "pool_state_before",
                decoder,
                target,
                calldata,
                pin,
                allow_empty=protocol == "v3",
                factory=pool_value.get("factory"),
            )
            key = (tuple(request["correlation"]["identity"].items()), decoder, target, calldata, pin)
            if key not in seen:
                seen.add(key)
                requests.append(request)
    return requests


def _solidity_error_reason(error: Any) -> str | None:
    if isinstance(error, Mapping):
        value = error.get("data")
    elif isinstance(error, str):
        # RpcClient raises deterministic EVM reverts as RpcError.  The
        # enrichment batch fallback preserves them as str(exc), so recover only
        # an explicitly keyed ABI payload.  Transport errors have no such
        # payload and must continue to fail the enrichment attempt.
        match = _RPC_REVERT_DATA_RE.search(error)
        value = match.group(1) if match is not None else None
    else:
        return None
    try:
        raw = _raw_hex(value, "RPC revert data")
    except ProtocolDecodeError:
        return None
    if len(raw) < 68 or raw[:4] != bytes.fromhex("08c379a0"):
        return None
    payload = raw[4:]
    offset = int.from_bytes(payload[:32], "big")
    size = int.from_bytes(payload[32:64], "big")
    padded = (size + 31) // 32 * 32
    if offset != 32 or len(payload) != 64 + padded:
        return None
    encoded = payload[64 : 64 + size]
    if any(payload[64 + size :]):
        return None
    try:
        return encoded.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _rpc_result(result: Any, correlation: Mapping[str, Any]) -> str | None:
    if isinstance(result, Mapping):
        if result.get("error") is not None:
            if (
                correlation.get("allow_missing")
                and _solidity_error_reason(result["error"]) == "Invalid token ID"
            ):
                return None
            raise ProtocolDecodeError(f"pinned eth_call failed: {result['error']}")
        result = result.get("result")
    if result == "0x" and correlation.get("allow_empty"):
        return None
    if not isinstance(result, str):
        raise ProtocolDecodeError("pinned eth_call returned no hex result")
    return result


def _decode_position_result(decoder: str, result: str | None) -> dict[str, Any]:
    if result is None:
        return {
            "exists": False,
            "liquidity": "0",
            "tokens_owed0": "0",
            "tokens_owed1": "0",
            "claims_empty": True,
            "absence_basis": "block_pinned_contract_or_token_absent",
        }
    if decoder == "v3_core_position":
        words = _words(result, "V3 core position result", 5)
        owed0 = _uint(words[3], 128, "V3 tokensOwed0")
        owed1 = _uint(words[4], 128, "V3 tokensOwed1")
        return {
            "exists": any(words),
            "liquidity": str(_uint(words[0], 128, "V3 position liquidity")),
            "fee_growth_inside0_last_x128": str(words[1]),
            "fee_growth_inside1_last_x128": str(words[2]),
            "tokens_owed0": str(owed0),
            "tokens_owed1": str(owed1),
            "claims_empty": owed0 == 0 and owed1 == 0,
            "source": "v3_core.positions",
        }
    if decoder == "v3_nfpm_position":
        words = _words(result, "V3 NFPM position result", 12)
        owed0 = _uint(words[10], 128, "NFPM tokensOwed0")
        owed1 = _uint(words[11], 128, "NFPM tokensOwed1")
        return {
            "exists": True,
            "nonce": str(_uint(words[0], 96, "NFPM nonce")),
            "operator": _word_address(words[1], "NFPM operator"),
            "token0": _word_address(words[2], "NFPM token0"),
            "token1": _word_address(words[3], "NFPM token1"),
            "fee_ppm": _uint(words[4], 24, "NFPM fee"),
            "tick_lower": _sint(words[5], 24, "NFPM tickLower"),
            "tick_upper": _sint(words[6], 24, "NFPM tickUpper"),
            "liquidity": str(_uint(words[7], 128, "NFPM liquidity")),
            "fee_growth_inside0_last_x128": str(words[8]),
            "fee_growth_inside1_last_x128": str(words[9]),
            "tokens_owed0": str(owed0),
            "tokens_owed1": str(owed1),
            "claims_empty": owed0 == 0 and owed1 == 0,
            "source": "verified_nfpm.positions",
        }
    if decoder == "v4_state_view_position":
        words = _words(result, "V4 StateView position result", 3)
        liquidity = _uint(words[0], 128, "V4 position liquidity")
        return {
            "exists": True if liquidity or words[1] or words[2] else None,
            "liquidity": str(liquidity),
            "fee_growth_inside0_last_x128": str(words[1]),
            "fee_growth_inside1_last_x128": str(words[2]),
            "tokens_owed0": None,
            "tokens_owed1": None,
            "claims_empty": True if liquidity == 0 else None,
            "claim_model": "fees_settled_on_modify_no_tokens_owed_storage",
            "source": "v4_state_view.getPositionInfo",
        }
    raise ProtocolDecodeError(f"unknown position result decoder {decoder}")


def decode_position_state_results(
    requests: Sequence[Mapping[str, Any]], results: Sequence[Any]
) -> list[dict[str, Any]]:
    """Decode pinned batch results into merge-by-immutable-identity updates."""
    if len(requests) != len(results):
        raise ProtocolDecodeError("pinned request/result count mismatch")
    updates: dict[tuple[str, str, int], dict[str, Any]] = {}
    for request, result_value in zip(requests, results):
        correlation = request.get("correlation")
        if not isinstance(correlation, Mapping):
            raise ProtocolDecodeError("pinned request correlation is missing")
        identity_value = correlation.get("identity")
        if not isinstance(identity_value, Mapping):
            raise ProtocolDecodeError("pinned event identity is missing")
        identity = (
            _hash(identity_value.get("block_hash"), "pinned block hash"),
            _hash(identity_value.get("tx_hash"), "pinned transaction hash"),
            _integer(identity_value.get("log_index"), "pinned log index"),
        )
        update = updates.setdefault(identity, {"identity": dict(identity_value), "data": {}})
        decoder = str(correlation.get("decoder"))
        result = _rpc_result(result_value, correlation)
        if decoder in {"v3_core_position", "v3_nfpm_position", "v4_state_view_position"}:
            field = str(correlation.get("field"))
            position = _decode_position_result(decoder, result)
            position["pinned_block"] = _integer(correlation.get("block_number"), "pinned block")
            if decoder == "v3_nfpm_position" and not position.get("exists"):
                missing_basis = correlation.get("missing_basis")
                if not isinstance(missing_basis, str):
                    raise ProtocolDecodeError(
                        "NFPM position absence lacks same-receipt lifecycle proof"
                    )
                position["absence_basis"] = missing_basis
            if decoder == "v3_nfpm_position" and position.get("exists"):
                comparisons = (
                    ("tick_lower", "expected_tick_lower"),
                    ("tick_upper", "expected_tick_upper"),
                    ("fee_ppm", "expected_fee_ppm"),
                    ("token0", "expected_token0"),
                    ("token1", "expected_token1"),
                )
                for actual_name, expected_name in comparisons:
                    expected = correlation.get(expected_name)
                    if expected is None:
                        continue
                    actual = position[actual_name]
                    if actual_name.startswith("token"):
                        expected = _address(expected, expected_name)
                    else:
                        expected = _integer(expected, expected_name)
                    if actual != expected:
                        raise ProtocolDecodeError(
                            f"NFPM token state {actual_name} does not match core event"
                        )
            update["data"][field] = position
            after_absence_basis = correlation.get(
                "position_after_absence_basis"
            )
            if after_absence_basis is not None:
                if (
                    decoder != "v4_state_view_position"
                    or field != "position_after"
                    or after_absence_basis
                    != "same_receipt_verified_v4_manager_burn"
                    or position.get("liquidity") != "0"
                ):
                    raise ProtocolDecodeError(
                        "verified V4 manager burn absence proof is inconsistent"
                    )
                # StateView reads the PoolManager's position record, whose fee
                # growth fields may remain after liquidity reaches zero. The
                # receipt burn proves NFT absence, not storage deletion.
                position.update(
                    nft_exists=False,
                    nft_absence_basis=after_absence_basis,
                )
            absence_basis = correlation.get("position_before_absence_basis")
            if absence_basis is not None:
                expected_basis = {
                    "v3_nfpm_position": "same_receipt_verified_nfpm_mint",
                    "v4_state_view_position": "same_receipt_verified_v4_manager_mint",
                }.get(decoder)
                parent_block = _integer(
                    correlation.get("position_before_block"),
                    "verified mint parent block",
                )
                if (
                    absence_basis != expected_basis
                    or field != "position_after"
                    or parent_block
                    != max(_integer(correlation.get("block_number"), "pinned block") - 1, 0)
                ):
                    raise ProtocolDecodeError(
                        "verified manager mint absence proof is inconsistent"
                    )
                if decoder == "v4_state_view_position":
                    before = {
                        "exists": False,
                        "liquidity": "0",
                        "fee_growth_inside0_last_x128": "0",
                        "fee_growth_inside1_last_x128": "0",
                        "tokens_owed0": None,
                        "tokens_owed1": None,
                        "claims_empty": True,
                        "claim_model": "fees_settled_on_modify_no_tokens_owed_storage",
                        "source": "verified_v4_manager_mint_absence",
                    }
                else:
                    before = _decode_position_result(decoder, None)
                before.update(
                    pinned_block=parent_block,
                    absence_basis=absence_basis,
                )
                update["data"]["position_before"] = before
            if decoder == "v3_nfpm_position" and position.get("exists"):
                update["data"]["position_identity"] = {
                    "manager": correlation.get("manager"),
                    "token_id": correlation.get("token_id"),
                    "token0": position["token0"], "token1": position["token1"],
                    "fee_ppm": position["fee_ppm"],
                    "tick_lower": position["tick_lower"], "tick_upper": position["tick_upper"],
                    "identity_basis": "verified_nfpm_token_state",
                }
            continue
        state = update["data"].setdefault(
            "pool_state_before",
            {"pinned_block": _integer(correlation.get("block_number"), "pinned block")},
        )
        if result is None:
            state.update(
                exists=False,
                source="pool_not_deployed_at_parent_block",
            )
            continue
        state["exists"] = True
        if decoder == "v3_pool_slot0":
            factory = _address(
                correlation.get("factory"), "V3 pool factory", strict=False
            )
            if factory not in CONCENTRATED_FACTORIES:
                raise ProtocolDecodeError(
                    "V3 slot0 factory identity is unresolved or unverified"
                )
            # Slipstream's primary ICLPoolState ABI returns six values and
            # deliberately has no protocol-fee word.  Its fee() and
            # unstakedFee() accessors are separate and do not prove a protocol
            # cut, so absence is represented as unknown rather than zero.
            slipstream = factory == SLIPSTREAM_FACTORY
            words = _words(result, "V3 slot0 result", 6 if slipstream else 7)
            state.update(
                sqrt_price_x96=str(_uint(words[0], 160, "V3 slot0 sqrtPriceX96")),
                tick=_sint(words[1], 24, "V3 slot0 tick"),
                observation_index=_uint(words[2], 16, "V3 slot0 observationIndex"),
                observation_cardinality=_uint(
                    words[3], 16, "V3 slot0 observationCardinality"
                ),
                observation_cardinality_next=_uint(
                    words[4], 16, "V3 slot0 observationCardinalityNext"
                ),
                unlocked=bool(
                    _uint(words[5 if slipstream else 6], 1, "V3 slot0 unlocked")
                ),
            )
            if slipstream:
                state.update(
                    fee_protocol=None,
                    fee_protocol0_divisor=None,
                    fee_protocol1_divisor=None,
                    fee_protocol_encoding="not_exposed_by_slipstream_slot0",
                    fee_protocol_basis="unknown_not_exposed",
                    source="slipstream_cl_pool.slot0+liquidity",
                )
            elif factory in _UINT32_FEE_PROTOCOL_FACTORIES:
                fee_protocol = _uint(words[5], 32, "extended V3 slot0 feeProtocol")
                state.update(
                    fee_protocol=fee_protocol,
                    fee_protocol0_ppm=fee_protocol & 0xFFFF,
                    fee_protocol1_ppm=fee_protocol >> 16,
                    fee_protocol_encoding=(
                        "pancake_uint16_ppm_each"
                        if factory == PANCAKE_V3_FACTORY
                        else "extended_v3_uint16_ppm_each"
                    ),
                    source=(
                        "pancake_v3_core.slot0+liquidity"
                        if factory == PANCAKE_V3_FACTORY
                        else "extended_v3_core.slot0+liquidity"
                    ),
                )
            elif factory == UNISWAP_V3_FACTORY:
                fee_protocol = _uint(words[5], 8, "V3 slot0 feeProtocol")
                state.update(
                    fee_protocol=fee_protocol,
                    fee_protocol0_divisor=fee_protocol & 0xF,
                    fee_protocol1_divisor=fee_protocol >> 4,
                    fee_protocol_encoding="uniswap_uint4_divisor_each",
                    source="v3_core.slot0+liquidity",
                )
            else:
                state.update(
                    fee_protocol=None,
                    fee_protocol_raw=str(words[5]),
                    fee_protocol0_divisor=None,
                    fee_protocol1_divisor=None,
                    fee_protocol_encoding="unknown_v3_fee_policy",
                    fee_protocol_basis="unknown_contract_abi",
                    source="v3_compatible_pool.slot0+liquidity",
                )
        elif decoder == "v3_pool_liquidity":
            words = _words(result, "V3 liquidity result", 1)
            state["liquidity"] = str(_uint(words[0], 128, "V3 active liquidity"))
        elif decoder == "v4_pool_slot0":
            words = _words(result, "V4 StateView slot0 result", 4)
            protocol_fee = _uint(words[2], 24, "V4 protocol fee")
            state.update(
                sqrt_price_x96=str(_uint(words[0], 160, "V4 sqrtPriceX96")),
                tick=_sint(words[1], 24, "V4 tick"),
                protocol_fee=protocol_fee,
                protocol_fee_zero_for_one=protocol_fee & 0xFFF,
                protocol_fee_one_for_zero=protocol_fee >> 12,
                lp_fee=_uint(words[3], 24, "V4 LP fee"),
                source="v4_state_view.getSlot0+getLiquidity",
            )
        elif decoder == "v4_pool_liquidity":
            words = _words(result, "V4 StateView liquidity result", 1)
            state["liquidity"] = str(_uint(words[0], 128, "V4 active liquidity"))
        else:
            raise ProtocolDecodeError(f"unknown pool-state result decoder {decoder}")
    return list(updates.values())


__all__ = [
    "EVENT_TOPICS", "NFT_EVENT_TOPICS", "NFT_MANAGER_ADDRESSES", "MANAGER_INFO",
    "POOL_MANAGER", "STATE_VIEW", "V4_POSITION_MANAGER",
    "V2_FACTORIES", "V3_FACTORIES", "CONCENTRATED_FACTORIES",
    "SLIPSTREAM_FACTORY", "PANCAKE_V3_FACTORY",
    "MODIFY_LIQUIDITY_SELECTOR", "SWAP_SELECTOR", "ProtocolDecodeError",
    "core_position_key", "decode_logs", "decode_gas_record", "manager_descriptor",
    "nft_position_key",
    "repair_v4_owners", "unknown_pool_candidates", "position_state_requests",
    "decode_position_state_results",
]
