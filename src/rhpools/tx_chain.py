"""Pure: no RPC, no clock, no policy. Every byte layout here was executed on an
anvil fork of the chain and is pinned by tests/test_tx_chain.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from typing import Any

from eth_abi import decode, encode
from eth_utils import keccak

from .lp_chain import CHAIN_ID, NATIVE, POOL_MANAGER, STATE_VIEW, USDG, WETH

UR = "0x8876789976decbfcbbbe364623c63652db8c0904"
PERMIT2 = "0x000000000022d473030f116ddee9f6b43ac78ba3"
UR_V2_FACTORY = "0x8bceaa40b9acdfaedf85adf4ff01f5ad6517937f"
UR_V3_FACTORY = "0x1f7d7550b1b028f7571e69a784071f0205fd2efa"
POSM = "0x58daec3116aae6d93017baaea7749052e8a04fa7"
PONS_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
NFPM_UNISWAP = "0x73991a25c818bf1f1128deaab1492d45638de0d3"
NFPM_PANCAKE = "0x46a15b0b27311cedf172ab29e4f4766fbe7f4364"
NFPM_GIGA = "0xa79f5775b0b49e51202c48ddf03f380faa96f641"
NFPM_BY_FACTORY = {
    UR_V3_FACTORY: NFPM_UNISWAP,
    "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865": NFPM_PANCAKE,
    "0xece6ecd61177336ea6fb9b17937ac439d85ee20b": NFPM_GIGA,
}

MSG_SENDER = "0x0000000000000000000000000000000000000001"
ADDRESS_THIS = "0x0000000000000000000000000000000000000002"
CONTRACT_BALANCE = 1 << 255
OPEN_DELTA = 0
MAX_UINT128 = (1 << 128) - 1
MAX_UINT160 = (1 << 160) - 1
MAX_UINT256 = (1 << 256) - 1


class Cmd(IntEnum):
    V3_SWAP_EXACT_IN = 0x00
    PERMIT2_TRANSFER_FROM = 0x02
    SWEEP = 0x04
    PAY_PORTION = 0x06
    V2_SWAP_EXACT_IN = 0x08
    PERMIT2_PERMIT = 0x0A
    WRAP_ETH = 0x0B
    UNWRAP_WETH = 0x0C
    V4_SWAP = 0x10


class V4Action(IntEnum):
    SWAP_EXACT_IN_SINGLE = 0x06
    SETTLE = 0x0B
    SETTLE_ALL = 0x0C
    TAKE = 0x0E


class PosmAction(IntEnum):
    INCREASE_LIQUIDITY = 0x00
    DECREASE_LIQUIDITY = 0x01
    MINT_POSITION = 0x02
    SETTLE_PAIR = 0x0D
    TAKE_PAIR = 0x11
    SWEEP = 0x14


def selector(signature: str) -> bytes:
    return keccak(text=signature)[:4]


def topic(signature: str) -> str:
    return "0x" + keccak(text=signature).hex()


SEL_EXECUTE = selector("execute(bytes,bytes[],uint256)")
SEL_MODIFY_LIQUIDITIES = selector("modifyLiquidities(bytes,uint256)")
SEL_MULTICALL = selector("multicall(bytes[])")
SEL_POSM_PERMIT_BATCH = selector("permitBatch(address,((address,uint160,uint48,uint48)[],address,uint256),bytes)")
SEL_NFPM_MINT = selector("mint((address,address,uint24,int24,int24,uint256,uint256,uint256,uint256,address,uint256))")
SEL_NFPM_INCREASE = selector("increaseLiquidity((uint256,uint256,uint256,uint256,uint256,uint256))")
SEL_NFPM_DECREASE = selector("decreaseLiquidity((uint256,uint128,uint256,uint256,uint256))")
SEL_NFPM_COLLECT = selector("collect((uint256,address,uint128,uint128))")
SEL_NFPM_POSITIONS = selector("positions(uint256)")
SEL_ERC20_APPROVE = selector("approve(address,uint256)")
SEL_ERC20_ALLOWANCE = selector("allowance(address,address)")
SEL_ERC20_BALANCE_OF = selector("balanceOf(address)")
SEL_PERMIT2_APPROVE = selector("approve(address,address,uint160,uint48)")
SEL_PERMIT2_ALLOWANCE = selector("allowance(address,address,address)")
SEL_PONS_LAUNCHES = bytes.fromhex("ad091230")
SEL_STATE_GET_SLOT0 = selector("getSlot0(bytes32)")
SEL_POSM_POOL_AND_POSITION = selector("getPoolAndPositionInfo(uint256)")
SEL_POSM_POSITION_LIQUIDITY = selector("getPositionLiquidity(uint256)")
SEL_V3_SLOT0 = selector("slot0()")
SEL_POOL_INITIALIZE = selector("initialize((address,address,uint24,int24,address),uint160)")

TOPIC_TRANSFER = topic("Transfer(address,address,uint256)")
TOPIC_V4_SWAP = topic("Swap(bytes32,address,int128,int128,uint160,uint128,int24,uint24)")
TOPIC_V3_SWAP = topic("Swap(address,address,int256,int256,uint160,uint128,int24)")
TOPIC_V2_SWAP = topic("Swap(address,uint256,uint256,uint256,uint256,address)")
TOPIC_V4_MODIFY_LIQUIDITY = topic("ModifyLiquidity(bytes32,address,int24,int24,int256,bytes32)")
TOPIC_NFPM_INCREASE = topic("IncreaseLiquidity(uint256,uint128,uint256,uint256)")
TOPIC_NFPM_DECREASE = topic("DecreaseLiquidity(uint256,uint128,uint256,uint256)")
TOPIC_NFPM_COLLECT = topic("Collect(uint256,address,uint256,uint256)")

PERMIT_DETAILS_TYPE = "(address,uint160,uint48,uint48)"
PERMIT_SINGLE_TYPE = f"({PERMIT_DETAILS_TYPE},address,uint256)"
PERMIT_BATCH_TYPE = f"({PERMIT_DETAILS_TYPE}[],address,uint256)"
POOL_KEY_TYPE = "(address,address,uint24,int24,address)"
V4_SINGLE_TYPE = f"({POOL_KEY_TYPE},bool,uint128,uint128,uint256,bytes)"
NFPM_MINT_TYPE = "(address,address,uint24,int24,int24,uint256,uint256,uint256,uint256,address,uint256)"
NFPM_INCREASE_TYPE = "(uint256,uint256,uint256,uint256,uint256,uint256)"
NFPM_DECREASE_TYPE = "(uint256,uint128,uint256,uint256,uint256)"
NFPM_COLLECT_TYPE = "(uint256,address,uint128,uint128)"
POSM_MINT_TYPES = [POOL_KEY_TYPE, "int24", "int24", "uint256", "uint128", "uint128", "address", "bytes"]
POSM_INCREASE_TYPES = ["uint256", "uint256", "uint128", "uint128", "bytes"]
POSM_DECREASE_TYPES = ["uint256", "uint256", "uint128", "uint128", "bytes"]


@dataclass(frozen=True)
class PoolKey:
    currency0: str
    currency1: str
    fee: int
    tick_spacing: int
    hooks: str

    def tuple(self) -> tuple[str, str, int, int, str]:
        return (self.currency0, self.currency1, self.fee, self.tick_spacing, self.hooks)

    def id(self) -> str:
        return "0x" + keccak(encode([POOL_KEY_TYPE], [self.tuple()])).hex()


@dataclass(frozen=True)
class PermitDetails:
    token: str
    amount: int
    expiration: int
    nonce: int

    def tuple(self) -> tuple[str, int, int, int]:
        return (self.token, self.amount, self.expiration, self.nonce)


@dataclass(frozen=True)
class PermitSingle:
    details: PermitDetails
    spender: str
    sig_deadline: int

    def tuple(self) -> tuple[Any, str, int]:
        return (self.details.tuple(), self.spender, self.sig_deadline)


@dataclass(frozen=True)
class PermitBatch:
    details: tuple[PermitDetails, ...]
    spender: str
    sig_deadline: int

    def tuple(self) -> tuple[Any, str, int]:
        return ([d.tuple() for d in self.details], self.spender, self.sig_deadline)


_PERMIT_TYPES = {
    "EIP712Domain": [
        {"name": "name", "type": "string"},
        {"name": "chainId", "type": "uint256"},
        {"name": "verifyingContract", "type": "address"},
    ],
    "PermitDetails": [
        {"name": "token", "type": "address"},
        {"name": "amount", "type": "uint160"},
        {"name": "expiration", "type": "uint48"},
        {"name": "nonce", "type": "uint48"},
    ],
}
_PERMIT_DOMAIN = {"name": "Permit2", "chainId": CHAIN_ID, "verifyingContract": PERMIT2}


def _details_json(details: PermitDetails) -> dict[str, str]:
    return {
        "token": details.token,
        "amount": str(details.amount),
        "expiration": str(details.expiration),
        "nonce": str(details.nonce),
    }


def permit_single_typed_data(permit: PermitSingle) -> dict[str, Any]:
    return {
        "types": {
            **_PERMIT_TYPES,
            "PermitSingle": [
                {"name": "details", "type": "PermitDetails"},
                {"name": "spender", "type": "address"},
                {"name": "sigDeadline", "type": "uint256"},
            ],
        },
        "primaryType": "PermitSingle",
        "domain": dict(_PERMIT_DOMAIN),
        "message": {
            "details": _details_json(permit.details),
            "spender": permit.spender,
            "sigDeadline": str(permit.sig_deadline),
        },
    }


def permit_batch_typed_data(permit: PermitBatch) -> dict[str, Any]:
    return {
        "types": {
            **_PERMIT_TYPES,
            "PermitBatch": [
                {"name": "details", "type": "PermitDetails[]"},
                {"name": "spender", "type": "address"},
                {"name": "sigDeadline", "type": "uint256"},
            ],
        },
        "primaryType": "PermitBatch",
        "domain": dict(_PERMIT_DOMAIN),
        "message": {
            "details": [_details_json(d) for d in permit.details],
            "spender": permit.spender,
            "sigDeadline": str(permit.sig_deadline),
        },
    }


def v3_path(tokens: tuple[str, ...], fees: tuple[int, ...]) -> bytes:
    out = bytes.fromhex(tokens[0][2:])
    for fee, token in zip(fees, tokens[1:], strict=True):
        out += fee.to_bytes(3, "big") + bytes.fromhex(token[2:])
    return out


def decode_v3_path(path: bytes) -> tuple[tuple[str, ...], tuple[int, ...]]:
    tokens = ["0x" + path[:20].hex()]
    fees: list[int] = []
    rest = path[20:]
    while rest:
        fees.append(int.from_bytes(rest[:3], "big"))
        tokens.append("0x" + rest[3:23].hex())
        rest = rest[23:]
    return tuple(tokens), tuple(fees)


class UrCommand:
    cmd: Cmd

    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass(frozen=True)
class WrapEth(UrCommand):
    recipient: str
    amount: int
    cmd = Cmd.WRAP_ETH

    def encode(self) -> bytes:
        return encode(["address", "uint256"], [self.recipient, self.amount])

    @classmethod
    def decode(cls, data: bytes) -> WrapEth:
        return cls(*decode(["address", "uint256"], data))


@dataclass(frozen=True)
class UnwrapWeth(UrCommand):
    recipient: str
    amount_min: int
    cmd = Cmd.UNWRAP_WETH

    def encode(self) -> bytes:
        return encode(["address", "uint256"], [self.recipient, self.amount_min])

    @classmethod
    def decode(cls, data: bytes) -> UnwrapWeth:
        return cls(*decode(["address", "uint256"], data))


@dataclass(frozen=True)
class PayPortion(UrCommand):
    token: str
    recipient: str
    bips: int
    cmd = Cmd.PAY_PORTION

    def encode(self) -> bytes:
        return encode(["address", "address", "uint256"], [self.token, self.recipient, self.bips])

    @classmethod
    def decode(cls, data: bytes) -> PayPortion:
        return cls(*decode(["address", "address", "uint256"], data))


@dataclass(frozen=True)
class Sweep(UrCommand):
    token: str
    recipient: str
    amount_min: int
    cmd = Cmd.SWEEP

    def encode(self) -> bytes:
        return encode(["address", "address", "uint256"], [self.token, self.recipient, self.amount_min])

    @classmethod
    def decode(cls, data: bytes) -> Sweep:
        return cls(*decode(["address", "address", "uint256"], data))


@dataclass(frozen=True)
class Permit2TransferFrom(UrCommand):
    token: str
    recipient: str
    amount: int
    cmd = Cmd.PERMIT2_TRANSFER_FROM

    def encode(self) -> bytes:
        return encode(["address", "address", "uint160"], [self.token, self.recipient, self.amount])

    @classmethod
    def decode(cls, data: bytes) -> Permit2TransferFrom:
        return cls(*decode(["address", "address", "uint160"], data))


@dataclass(frozen=True)
class Permit2Permit(UrCommand):
    permit: PermitSingle
    signature: bytes
    cmd = Cmd.PERMIT2_PERMIT

    def encode(self) -> bytes:
        return encode([PERMIT_SINGLE_TYPE, "bytes"], [self.permit.tuple(), self.signature])

    @classmethod
    def decode(cls, data: bytes) -> Permit2Permit:
        (details, spender, sig_deadline), signature = decode([PERMIT_SINGLE_TYPE, "bytes"], data)
        return cls(PermitSingle(PermitDetails(*details), spender, sig_deadline), signature)


@dataclass(frozen=True)
class V3Swap(UrCommand):
    """Robinhood ABI: the trailing ``uint256[] minHopPriceX36`` is required; it stays empty."""

    recipient: str
    amount_in: int
    min_out: int
    path: bytes
    payer_is_user: bool
    cmd = Cmd.V3_SWAP_EXACT_IN

    def encode(self) -> bytes:
        return encode(
            ["address", "uint256", "uint256", "bytes", "bool", "uint256[]"],
            [self.recipient, self.amount_in, self.min_out, self.path, self.payer_is_user, []],
        )

    @classmethod
    def decode(cls, data: bytes) -> V3Swap:
        recipient, amount_in, min_out, path, payer_is_user, _ = decode(
            ["address", "uint256", "uint256", "bytes", "bool", "uint256[]"], data
        )
        return cls(recipient, amount_in, min_out, path, payer_is_user)


@dataclass(frozen=True)
class V2Swap(UrCommand):
    recipient: str
    amount_in: int
    min_out: int
    path: tuple[str, ...]
    payer_is_user: bool
    cmd = Cmd.V2_SWAP_EXACT_IN

    def encode(self) -> bytes:
        return encode(
            ["address", "uint256", "uint256", "address[]", "bool", "uint256[]"],
            [self.recipient, self.amount_in, self.min_out, list(self.path), self.payer_is_user, []],
        )

    @classmethod
    def decode(cls, data: bytes) -> V2Swap:
        recipient, amount_in, min_out, path, payer_is_user, _ = decode(
            ["address", "uint256", "uint256", "address[]", "bool", "uint256[]"], data
        )
        return cls(recipient, amount_in, min_out, tuple(path), payer_is_user)


class V4Param:
    action: V4Action

    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass(frozen=True)
class V4Settle(V4Param):
    currency: str
    amount: int
    payer_is_user: bool
    action = V4Action.SETTLE

    def encode(self) -> bytes:
        return encode(["address", "uint256", "bool"], [self.currency, self.amount, self.payer_is_user])

    @classmethod
    def decode(cls, data: bytes) -> V4Settle:
        return cls(*decode(["address", "uint256", "bool"], data))


@dataclass(frozen=True)
class V4SettleAll(V4Param):
    currency: str
    max_amount: int
    action = V4Action.SETTLE_ALL

    def encode(self) -> bytes:
        return encode(["address", "uint256"], [self.currency, self.max_amount])

    @classmethod
    def decode(cls, data: bytes) -> V4SettleAll:
        return cls(*decode(["address", "uint256"], data))


@dataclass(frozen=True)
class V4Take(V4Param):
    currency: str
    recipient: str
    amount: int
    action = V4Action.TAKE

    def encode(self) -> bytes:
        return encode(["address", "address", "uint256"], [self.currency, self.recipient, self.amount])

    @classmethod
    def decode(cls, data: bytes) -> V4Take:
        return cls(*decode(["address", "address", "uint256"], data))


@dataclass(frozen=True)
class V4SwapExactInSingle(V4Param):
    """Robinhood ABI: ``(PoolKey,bool,uint128,uint128,uint256 minHopPriceX36,bytes hookData)``."""

    key: PoolKey
    zero_for_one: bool
    amount_in: int
    min_out: int
    action = V4Action.SWAP_EXACT_IN_SINGLE

    def encode(self) -> bytes:
        return encode([V4_SINGLE_TYPE], [(self.key.tuple(), self.zero_for_one, self.amount_in, self.min_out, 0, b"")])

    @classmethod
    def decode(cls, data: bytes) -> V4SwapExactInSingle:
        (key, zero_for_one, amount_in, min_out, _, _), = decode([V4_SINGLE_TYPE], data)
        return cls(PoolKey(*key), zero_for_one, amount_in, min_out)


_V4_PARAMS: dict[int, type] = {
    V4Action.SETTLE: V4Settle,
    V4Action.SETTLE_ALL: V4SettleAll,
    V4Action.TAKE: V4Take,
    V4Action.SWAP_EXACT_IN_SINGLE: V4SwapExactInSingle,
}


@dataclass(frozen=True)
class V4Swap(UrCommand):
    params: tuple[V4Param, ...]
    cmd = Cmd.V4_SWAP

    def encode(self) -> bytes:
        actions = bytes(p.action for p in self.params)
        return encode(["bytes", "bytes[]"], [actions, [p.encode() for p in self.params]])

    @classmethod
    def decode(cls, data: bytes) -> V4Swap:
        actions, params = decode(["bytes", "bytes[]"], data)
        return cls(tuple(_V4_PARAMS[a].decode(p) for a, p in zip(actions, params, strict=True)))


_UR_COMMANDS: dict[int, type] = {
    Cmd.V3_SWAP_EXACT_IN: V3Swap,
    Cmd.PERMIT2_TRANSFER_FROM: Permit2TransferFrom,
    Cmd.SWEEP: Sweep,
    Cmd.PAY_PORTION: PayPortion,
    Cmd.V2_SWAP_EXACT_IN: V2Swap,
    Cmd.PERMIT2_PERMIT: Permit2Permit,
    Cmd.WRAP_ETH: WrapEth,
    Cmd.UNWRAP_WETH: UnwrapWeth,
    Cmd.V4_SWAP: V4Swap,
}


def ur_execute(commands: tuple[UrCommand, ...], deadline: int) -> bytes:
    cmds = bytes(c.cmd for c in commands)
    return SEL_EXECUTE + encode(["bytes", "bytes[]", "uint256"], [cmds, [c.encode() for c in commands], deadline])


def decode_ur_execute(data: bytes) -> tuple[tuple[UrCommand, ...], int]:
    if data[:4] != SEL_EXECUTE:
        raise ValueError("not a UniversalRouter execute call")
    cmds, inputs, deadline = decode(["bytes", "bytes[]", "uint256"], data[4:])
    return tuple(_UR_COMMANDS[c].decode(i) for c, i in zip(cmds, inputs, strict=True)), deadline


class PosmParam:
    action: PosmAction

    def encode(self) -> bytes:
        raise NotImplementedError


@dataclass(frozen=True)
class PosmMint(PosmParam):
    """PosM decodes MINT params flat (abi.encode of the fields), not as one wrapped tuple."""

    key: PoolKey
    tick_lower: int
    tick_upper: int
    liquidity: int
    amount0_max: int
    amount1_max: int
    owner: str
    action = PosmAction.MINT_POSITION

    def encode(self) -> bytes:
        return encode(POSM_MINT_TYPES, [self.key.tuple(), self.tick_lower, self.tick_upper, self.liquidity, self.amount0_max, self.amount1_max, self.owner, b""])

    @classmethod
    def decode(cls, data: bytes) -> PosmMint:
        key, lower, upper, liquidity, max0, max1, owner, _ = decode(POSM_MINT_TYPES, data)
        return cls(PoolKey(*key), lower, upper, liquidity, max0, max1, owner)


@dataclass(frozen=True)
class PosmIncrease(PosmParam):
    token_id: int
    liquidity: int
    amount0_max: int
    amount1_max: int
    action = PosmAction.INCREASE_LIQUIDITY

    def encode(self) -> bytes:
        return encode(POSM_INCREASE_TYPES, [self.token_id, self.liquidity, self.amount0_max, self.amount1_max, b""])

    @classmethod
    def decode(cls, data: bytes) -> PosmIncrease:
        return cls(*decode(POSM_INCREASE_TYPES, data)[:4])


@dataclass(frozen=True)
class PosmDecrease(PosmParam):
    token_id: int
    liquidity: int
    amount0_min: int
    amount1_min: int
    action = PosmAction.DECREASE_LIQUIDITY

    def encode(self) -> bytes:
        return encode(POSM_DECREASE_TYPES, [self.token_id, self.liquidity, self.amount0_min, self.amount1_min, b""])

    @classmethod
    def decode(cls, data: bytes) -> PosmDecrease:
        return cls(*decode(POSM_DECREASE_TYPES, data)[:4])


@dataclass(frozen=True)
class PosmSettlePair(PosmParam):
    currency0: str
    currency1: str
    action = PosmAction.SETTLE_PAIR

    def encode(self) -> bytes:
        return encode(["address", "address"], [self.currency0, self.currency1])

    @classmethod
    def decode(cls, data: bytes) -> PosmSettlePair:
        return cls(*decode(["address", "address"], data))


@dataclass(frozen=True)
class PosmTakePair(PosmParam):
    currency0: str
    currency1: str
    recipient: str
    action = PosmAction.TAKE_PAIR

    def encode(self) -> bytes:
        return encode(["address", "address", "address"], [self.currency0, self.currency1, self.recipient])

    @classmethod
    def decode(cls, data: bytes) -> PosmTakePair:
        return cls(*decode(["address", "address", "address"], data))


@dataclass(frozen=True)
class PosmSweep(PosmParam):
    currency: str
    recipient: str
    action = PosmAction.SWEEP

    def encode(self) -> bytes:
        return encode(["address", "address"], [self.currency, self.recipient])

    @classmethod
    def decode(cls, data: bytes) -> PosmSweep:
        return cls(*decode(["address", "address"], data))


_POSM_PARAMS: dict[int, type] = {
    PosmAction.MINT_POSITION: PosmMint,
    PosmAction.INCREASE_LIQUIDITY: PosmIncrease,
    PosmAction.DECREASE_LIQUIDITY: PosmDecrease,
    PosmAction.SETTLE_PAIR: PosmSettlePair,
    PosmAction.TAKE_PAIR: PosmTakePair,
    PosmAction.SWEEP: PosmSweep,
}


def posm_modify_liquidities(params: tuple[PosmParam, ...], deadline: int) -> bytes:
    actions = bytes(p.action for p in params)
    unlock = encode(["bytes", "bytes[]"], [actions, [p.encode() for p in params]])
    return SEL_MODIFY_LIQUIDITIES + encode(["bytes", "uint256"], [unlock, deadline])


def decode_posm_modify_liquidities(data: bytes) -> tuple[tuple[PosmParam, ...], int]:
    if data[:4] != SEL_MODIFY_LIQUIDITIES:
        raise ValueError("not a PositionManager modifyLiquidities call")
    unlock, deadline = decode(["bytes", "uint256"], data[4:])
    actions, params = decode(["bytes", "bytes[]"], unlock)
    return tuple(_POSM_PARAMS[a].decode(p) for a, p in zip(actions, params, strict=True)), deadline


def posm_permit_batch(owner: str, permit: PermitBatch, signature: bytes) -> bytes:
    return SEL_POSM_PERMIT_BATCH + encode(["address", PERMIT_BATCH_TYPE, "bytes"], [owner, permit.tuple(), signature])


def multicall(calls: tuple[bytes, ...]) -> bytes:
    return SEL_MULTICALL + encode(["bytes[]"], [list(calls)])


def decode_multicall(data: bytes) -> tuple[bytes, ...]:
    if data[:4] != SEL_MULTICALL:
        raise ValueError("not a multicall")
    return tuple(decode(["bytes[]"], data[4:])[0])


@dataclass(frozen=True)
class NfpmMint:
    token0: str
    token1: str
    fee: int
    tick_lower: int
    tick_upper: int
    amount0_desired: int
    amount1_desired: int
    amount0_min: int
    amount1_min: int
    recipient: str
    deadline: int

    def encode(self) -> bytes:
        return SEL_NFPM_MINT + encode([NFPM_MINT_TYPE], [(self.token0, self.token1, self.fee, self.tick_lower, self.tick_upper, self.amount0_desired, self.amount1_desired, self.amount0_min, self.amount1_min, self.recipient, self.deadline)])

    @classmethod
    def decode(cls, data: bytes) -> NfpmMint:
        return cls(*decode([NFPM_MINT_TYPE], data[4:])[0])


@dataclass(frozen=True)
class NfpmIncrease:
    token_id: int
    amount0_desired: int
    amount1_desired: int
    amount0_min: int
    amount1_min: int
    deadline: int

    def encode(self) -> bytes:
        return SEL_NFPM_INCREASE + encode([NFPM_INCREASE_TYPE], [(self.token_id, self.amount0_desired, self.amount1_desired, self.amount0_min, self.amount1_min, self.deadline)])

    @classmethod
    def decode(cls, data: bytes) -> NfpmIncrease:
        return cls(*decode([NFPM_INCREASE_TYPE], data[4:])[0])


@dataclass(frozen=True)
class NfpmDecrease:
    token_id: int
    liquidity: int
    amount0_min: int
    amount1_min: int
    deadline: int

    def encode(self) -> bytes:
        return SEL_NFPM_DECREASE + encode([NFPM_DECREASE_TYPE], [(self.token_id, self.liquidity, self.amount0_min, self.amount1_min, self.deadline)])

    @classmethod
    def decode(cls, data: bytes) -> NfpmDecrease:
        return cls(*decode([NFPM_DECREASE_TYPE], data[4:])[0])


@dataclass(frozen=True)
class NfpmCollect:
    token_id: int
    recipient: str
    amount0_max: int = MAX_UINT128
    amount1_max: int = MAX_UINT128

    def encode(self) -> bytes:
        return SEL_NFPM_COLLECT + encode([NFPM_COLLECT_TYPE], [(self.token_id, self.recipient, self.amount0_max, self.amount1_max)])

    @classmethod
    def decode(cls, data: bytes) -> NfpmCollect:
        return cls(*decode([NFPM_COLLECT_TYPE], data[4:])[0])


_NFPM_CALLS: dict[bytes, type] = {
    SEL_NFPM_MINT: NfpmMint,
    SEL_NFPM_INCREASE: NfpmIncrease,
    SEL_NFPM_DECREASE: NfpmDecrease,
    SEL_NFPM_COLLECT: NfpmCollect,
}


def decode_nfpm_call(data: bytes) -> NfpmMint | NfpmIncrease | NfpmDecrease | NfpmCollect:
    kind = _NFPM_CALLS.get(data[:4])
    if kind is None:
        raise ValueError("unknown NFPM call")
    return kind.decode(data)


def erc20_approve(spender: str, amount: int) -> bytes:
    return SEL_ERC20_APPROVE + encode(["address", "uint256"], [spender, amount])


def erc20_allowance(owner: str, spender: str) -> bytes:
    return SEL_ERC20_ALLOWANCE + encode(["address", "address"], [owner, spender])


def erc20_balance_of(owner: str) -> bytes:
    return SEL_ERC20_BALANCE_OF + encode(["address"], [owner])


def permit2_approve(token: str, spender: str, amount: int, expiration: int) -> bytes:
    return SEL_PERMIT2_APPROVE + encode(["address", "address", "uint160", "uint48"], [token, spender, amount, expiration])


def permit2_allowance(owner: str, token: str, spender: str) -> bytes:
    return SEL_PERMIT2_ALLOWANCE + encode(["address", "address", "address"], [owner, token, spender])


def pons_launches(pool_id: str) -> bytes:
    return SEL_PONS_LAUNCHES + bytes.fromhex(pool_id[2:])


def state_view_slot0(pool_id: str) -> bytes:
    return SEL_STATE_GET_SLOT0 + bytes.fromhex(pool_id[2:])


def posm_pool_and_position(token_id: int) -> bytes:
    return SEL_POSM_POOL_AND_POSITION + encode(["uint256"], [token_id])


def posm_position_liquidity(token_id: int) -> bytes:
    return SEL_POSM_POSITION_LIQUIDITY + encode(["uint256"], [token_id])


def nfpm_positions(token_id: int) -> bytes:
    return SEL_NFPM_POSITIONS + encode(["uint256"], [token_id])


def pool_manager_initialize(key: PoolKey, sqrt_price_x96: int) -> bytes:
    return SEL_POOL_INITIALIZE + encode([POOL_KEY_TYPE, "uint160"], [key.tuple(), sqrt_price_x96])


HOOK_FLAG_NAMES = (
    "AFTER_REMOVE_LIQUIDITY_RETURNS_DELTA",
    "AFTER_ADD_LIQUIDITY_RETURNS_DELTA",
    "AFTER_SWAP_RETURNS_DELTA",
    "BEFORE_SWAP_RETURNS_DELTA",
    "AFTER_DONATE",
    "BEFORE_DONATE",
    "AFTER_SWAP",
    "BEFORE_SWAP",
    "AFTER_REMOVE_LIQUIDITY",
    "BEFORE_REMOVE_LIQUIDITY",
    "AFTER_ADD_LIQUIDITY",
    "BEFORE_ADD_LIQUIDITY",
    "AFTER_INITIALIZE",
    "BEFORE_INITIALIZE",
)
ADD_GOVERNING_FLAGS = frozenset({"BEFORE_ADD_LIQUIDITY", "AFTER_ADD_LIQUIDITY_RETURNS_DELTA"})


def hook_flags(hook: str) -> frozenset[str]:
    bits = int(hook, 16) & 0x3FFF
    return frozenset(name for i, name in enumerate(HOOK_FLAG_NAMES) if bits >> i & 1)


@dataclass(frozen=True)
class RevertKind:
    kind: str
    selector: str
    detail: str = ""


_ERROR_STRING = selector("Error(string)")
_WRAPPED_ERROR = selector("WrappedError(address,bytes4,bytes,bytes)")
_EXECUTION_FAILED = selector("ExecutionFailed(uint256,bytes)")
_KNOWN_SELECTORS: dict[bytes, str] = {
    selector("TransactionDeadlinePassed()"): "expired",
    selector("DeadlinePassed(uint256)"): "expired",
    selector("SignatureExpired(uint256)"): "expired",
    selector("AllowanceExpired(uint256)"): "expired",
    selector("V3TooLittleReceived()"): "slippage",
    selector("V2TooLittleReceived()"): "slippage",
    selector("V4TooLittleReceived(uint256,uint256)"): "slippage",
    selector("InsufficientETH()"): "slippage",
    selector("InsufficientToken()"): "slippage",
    selector("MinimumAmountInsufficient(uint128,uint128)"): "slippage",
    selector("MaximumAmountExceeded(uint128,uint128)"): "slippage",
    bytes.fromhex("4713c18b"): "slippage",
    selector("InvalidSignature()"): "bad_signature",
    selector("InvalidSigner()"): "bad_signature",
    selector("InvalidContractSignature()"): "bad_signature",
    selector("InvalidSignatureLength()"): "bad_signature",
    selector("InvalidNonce()"): "bad_signature",
    selector("InsufficientAllowance(uint256)"): "approve_pending",
    selector("NotApproved(address)"): "approve_pending",
    selector("SliceOutOfBounds()"): "encoding",
    bytes.fromhex("383ef61c"): "encoding",
}
_STRING_REASONS: dict[str, str] = {
    "Price slippage check": "slippage",
    "Transaction too old": "expired",
    "Too little received": "slippage",
    "STF": "insufficient_balance",
    "TRANSFER_FROM_FAILED": "insufficient_balance",
    "ERC20: transfer amount exceeds balance": "insufficient_balance",
    "ERC20: transfer amount exceeds allowance": "approve_pending",
    "ERC20: insufficient allowance": "approve_pending",
}


def decode_revert(data: bytes) -> RevertKind:
    if not data:
        return RevertKind("unknown", "0x")
    sel = data[:4]
    hexsel = "0x" + sel.hex()
    if sel == _ERROR_STRING:
        try:
            reason = decode(["string"], data[4:])[0]
        except Exception:
            reason = ""
        return RevertKind(_STRING_REASONS.get(reason, "unknown"), hexsel, reason)
    if sel == _EXECUTION_FAILED:
        try:
            index, inner = decode(["uint256", "bytes"], data[4:])
        except Exception:
            return RevertKind("unknown", hexsel)
        inner_kind = decode_revert(inner)
        return RevertKind(inner_kind.kind, inner_kind.selector, f"command {index}: {inner_kind.detail}".strip())
    if sel == _WRAPPED_ERROR:
        try:
            target, hook_sel, reason, _ = decode(["address", "bytes4", "bytes", "bytes"], data[4:])
        except Exception:
            return RevertKind("hook_reverted", hexsel)
        return RevertKind("hook_reverted", hexsel, f"{target} {hook_sel.hex()} {reason.hex()}")
    kind = _KNOWN_SELECTORS.get(sel)
    if kind is None:
        return RevertKind("unknown", hexsel)
    return RevertKind(kind, hexsel)


__all__ = [
    "ADDRESS_THIS", "ADD_GOVERNING_FLAGS", "CHAIN_ID", "CONTRACT_BALANCE", "Cmd", "MAX_UINT128",
    "MAX_UINT160", "MAX_UINT256", "MSG_SENDER", "NATIVE", "NFPM_BY_FACTORY", "NFPM_GIGA",
    "NFPM_PANCAKE", "NFPM_UNISWAP", "NfpmCollect", "NfpmDecrease", "NfpmIncrease", "NfpmMint",
    "OPEN_DELTA", "PERMIT2", "PONS_HOOK", "POOL_MANAGER", "POSM", "PayPortion", "Permit2Permit",
    "Permit2TransferFrom", "PermitBatch", "PermitDetails", "PermitSingle", "PoolKey",
    "PosmAction", "PosmDecrease", "PosmIncrease", "PosmMint", "PosmParam", "PosmSettlePair",
    "PosmSweep", "PosmTakePair", "RevertKind", "STATE_VIEW", "Sweep", "TOPIC_NFPM_COLLECT",
    "TOPIC_NFPM_DECREASE", "TOPIC_NFPM_INCREASE", "TOPIC_TRANSFER", "TOPIC_V2_SWAP",
    "TOPIC_V3_SWAP", "TOPIC_V4_MODIFY_LIQUIDITY", "TOPIC_V4_SWAP", "UR", "UR_V2_FACTORY",
    "UR_V3_FACTORY", "USDG", "UnwrapWeth", "UrCommand", "V2Swap", "V3Swap", "V4Action",
    "V4Param", "V4Settle", "V4SettleAll", "V4Swap", "V4SwapExactInSingle", "V4Take", "WETH",
    "WrapEth", "decode_multicall", "decode_nfpm_call", "decode_posm_modify_liquidities",
    "decode_revert", "decode_ur_execute", "decode_v3_path", "erc20_allowance",
    "erc20_approve", "erc20_balance_of", "hook_flags", "multicall", "nfpm_positions",
    "permit2_allowance", "permit2_approve", "permit_batch_typed_data",
    "permit_single_typed_data", "pons_launches", "pool_manager_initialize",
    "posm_modify_liquidities", "posm_permit_batch", "posm_pool_and_position",
    "posm_position_liquidity", "selector", "state_view_slot0", "topic", "ur_execute", "v3_path",
]
