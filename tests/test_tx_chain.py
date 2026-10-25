"""Golden calldata pinned from bytes that executed on an anvil fork of chain 4663.

tests/fixtures/tx_golden.json was captured by sending each shape from a fresh
EOA (0x3194…8b50) and asserting receipt status 1. Deadline and Permit2
expiry are the constant 2_000_000_000 so signatures are reproducible.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from rhpools import tx_chain as tc
from rhpools.tx_chain import (
    ADDRESS_THIS, CONTRACT_BALANCE, MSG_SENDER, NATIVE, NFPM_UNISWAP, OPEN_DELTA, PERMIT2,
    PONS_HOOK, POSM, UR, USDG, WETH, NfpmCollect, NfpmDecrease, NfpmMint, PayPortion,
    Permit2Permit, Permit2TransferFrom, PermitBatch, PermitDetails, PermitSingle, PoolKey,
    PosmDecrease, PosmMint, PosmSettlePair, PosmSweep, PosmTakePair, Sweep, UnwrapWeth, V2Swap,
    V3Swap, V4Settle, V4Swap, V4SwapExactInSingle, V4Take, WrapEth, decode_multicall,
    decode_nfpm_call, decode_posm_modify_liquidities, decode_revert, decode_ur_execute,
    decode_v3_path, hook_flags, multicall, permit_batch_typed_data, permit_single_typed_data,
    posm_modify_liquidities, posm_permit_batch, ur_execute, v3_path,
)

GOLDEN = json.loads((Path(__file__).parent / "fixtures" / "tx_golden.json").read_text())
USER = "0x3194a88fea72b9fa6a981c4265834c613a2c8b50"
FEE_TO = "0x00000000000000000000000000000000000fee75"
ITH = "0xb3377f953994c6fa04383a64d88aa8122e81fd62"
PONS_KEY = PoolKey(NATIVE, ITH, 0, 200, PONS_HOOK)
DL = 2_000_000_000


def _sig(name: str) -> bytes:
    return bytes.fromhex(GOLDEN[name][2:])


def _permit(token: str, amount: int, nonce: int) -> PermitSingle:
    return PermitSingle(PermitDetails(token, amount, DL, nonce), UR, DL)


def _check(name: str, to: str, data: bytes, value: int = 0) -> None:
    entry = GOLDEN[name]
    assert entry["to"] == to
    assert entry["value"] == value
    assert "0x" + data.hex() == entry["data"]


T1 = (
    WrapEth(ADDRESS_THIS, CONTRACT_BALANCE),
    PayPortion(WETH, FEE_TO, 75),
    V3Swap(MSG_SENDER, CONTRACT_BALANCE, 1, v3_path((WETH, USDG), (100,)), False),
)
T2 = (
    Permit2Permit(_permit(USDG, 1_000 * 10**6, GOLDEN["t2_permit_nonce"]), _sig("t2_permit_signature")),
    Permit2TransferFrom(USDG, ADDRESS_THIS, 1_000 * 10**6),
    V3Swap(ADDRESS_THIS, CONTRACT_BALANCE, 0, v3_path((USDG, WETH), (100,)), False),
    PayPortion(WETH, FEE_TO, 75),
    UnwrapWeth(MSG_SENDER, 1),
)
T3 = (
    PayPortion(NATIVE, FEE_TO, 75),
    V4Swap((V4Settle(NATIVE, CONTRACT_BALANCE, False), V4SwapExactInSingle(PONS_KEY, True, OPEN_DELTA, 1), V4Take(ITH, MSG_SENDER, OPEN_DELTA))),
)
T4 = (
    Permit2Permit(_permit(ITH, GOLDEN["t4_ith_in"], 0), _sig("t4_permit_signature")),
    Permit2TransferFrom(ITH, ADDRESS_THIS, GOLDEN["t4_ith_in"]),
    V4Swap((V4Settle(ITH, CONTRACT_BALANCE, False), V4SwapExactInSingle(PONS_KEY, False, OPEN_DELTA, 1), V4Take(NATIVE, ADDRESS_THIS, OPEN_DELTA))),
    PayPortion(NATIVE, FEE_TO, 75),
    Sweep(NATIVE, MSG_SENDER, 1),
)
T5 = (
    Permit2Permit(_permit(USDG, 50 * 10**6, GOLDEN["t5_permit_nonce"]), _sig("t5_permit_signature")),
    Permit2TransferFrom(USDG, ADDRESS_THIS, 50 * 10**6),
    V2Swap(ADDRESS_THIS, CONTRACT_BALANCE, 1, (USDG, WETH), False),
    PayPortion(WETH, FEE_TO, 75),
    UnwrapWeth(MSG_SENDER, 1),
)
B1 = (
    Permit2Permit(_permit(USDG, 500 * 10**6, GOLDEN["b1_permit_nonce"]), _sig("b1_permit_signature")),
    Permit2TransferFrom(USDG, ADDRESS_THIS, 500 * 10**6),
    PayPortion(USDG, FEE_TO, 75),
    V3Swap(ADDRESS_THIS, CONTRACT_BALANCE, 0, v3_path((USDG, WETH), (100,)), False),
    UnwrapWeth(ADDRESS_THIS, 0),
    V4Swap((V4Settle(NATIVE, CONTRACT_BALANCE, False), V4SwapExactInSingle(PONS_KEY, True, OPEN_DELTA, 1), V4Take(ITH, MSG_SENDER, OPEN_DELTA))),
)
SWAP_GOLDENS = {
    "t1_eth_usdg_v3_fee_in": (T1, 10**18),
    "t2_usdg_eth_v3_permit_fee_out": (T2, 0),
    "t3_eth_ith_v4_fee_in": (T3, 10**16),
    "t4_ith_eth_v4_permit_fee_out": (T4, 0),
    "t5_usdg_eth_v2_permit_fee_out": (T5, 0),
    "b1_usdg_ith_bridged_fee_in": (B1, 0),
}


@pytest.mark.parametrize("name", sorted(SWAP_GOLDENS))
def test_universal_router_golden_bytes(name):
    commands, value = SWAP_GOLDENS[name]
    data = ur_execute(commands, DL)
    _check(name, UR, data, value)
    assert decode_ur_execute(data) == (commands, DL)


def test_posm_mint_multicall_permit_batch_golden():
    lower, upper = GOLDEN["h2_ticks"]
    batch = PermitBatch((PermitDetails(ITH, GOLDEN["h2_ith_max"], DL, 0),), POSM, DL)
    params = (PosmMint(PONS_KEY, lower, upper, 10**15, 10**17, GOLDEN["h2_ith_max"], USER), PosmSettlePair(NATIVE, ITH), PosmSweep(NATIVE, USER))
    modify = posm_modify_liquidities(params, DL)
    data = multicall((posm_permit_batch(USER, batch, _sig("h2_permit_batch_signature")), modify))
    _check("h2_posm_mint_multicall_permit_batch", POSM, data, 10**17)
    assert decode_multicall(data)[1] == modify
    assert decode_posm_modify_liquidities(modify) == (params, DL)


def test_posm_decrease_take_pair_golden():
    params = (PosmDecrease(GOLDEN["h2_token_id"], 10**15 // 2, 0, 0), PosmTakePair(NATIVE, ITH, USER))
    data = posm_modify_liquidities(params, DL)
    _check("h3_posm_decrease_take_pair", POSM, data)
    assert decode_posm_modify_liquidities(data) == (params, DL)


def test_nfpm_mint_and_decrease_collect_golden():
    lower, upper = GOLDEN["h5_ticks"]
    mint = NfpmMint(WETH, USDG, 100, lower, upper, 10**16, GOLDEN["h5_usdg_desired"], 0, 0, USER, DL)
    _check("h5_nfpm_mint", NFPM_UNISWAP, mint.encode())
    assert decode_nfpm_call(mint.encode()) == mint
    decrease = NfpmDecrease(GOLDEN["h5_token_id"], GOLDEN["h5_liquidity"], 0, 0, DL)
    collect = NfpmCollect(GOLDEN["h5_token_id"], USER)
    data = multicall((decrease.encode(), collect.encode()))
    _check("h6_nfpm_multicall_decrease_collect", NFPM_UNISWAP, data)
    assert [decode_nfpm_call(c) for c in decode_multicall(data)] == [decrease, collect]


def test_pons_pool_id_matches_market_row():
    assert PONS_KEY.id() == "0xeda3dbcd4b745a70d04a92293bb7dcb3d9de234bd59e2773574a259cfe6f3fb0"


def test_v3_path_round_trip():
    path = v3_path((USDG, WETH, ITH), (100, 10000))
    assert len(path) == 20 + 23 * 2
    assert decode_v3_path(path) == ((USDG, WETH, ITH), (100, 10000))


def test_permit_typed_data_shapes():
    single = permit_single_typed_data(_permit(USDG, 5, 7))
    assert single["primaryType"] == "PermitSingle"
    assert single["domain"] == {"name": "Permit2", "chainId": 4663, "verifyingContract": PERMIT2}
    assert single["message"] == {"details": {"token": USDG, "amount": "5", "expiration": str(DL), "nonce": "7"}, "spender": UR, "sigDeadline": str(DL)}
    batch = permit_batch_typed_data(PermitBatch((PermitDetails(ITH, 1, DL, 0), PermitDetails(USDG, 2, DL, 3)), POSM, DL))
    assert batch["primaryType"] == "PermitBatch"
    assert batch["types"]["PermitBatch"][0] == {"name": "details", "type": "PermitDetails[]"}
    assert [d["token"] for d in batch["message"]["details"]] == [ITH, USDG]
    assert batch["message"]["spender"] == POSM


def test_hook_flags():
    assert hook_flags(PONS_HOOK) == {"BEFORE_INITIALIZE", "AFTER_SWAP", "AFTER_SWAP_RETURNS_DELTA"}
    assert hook_flags(NATIVE) == frozenset()
    assert hook_flags("0x0000000000000000000000000000000000000800") == {"BEFORE_ADD_LIQUIDITY"}
    assert hook_flags("0x0000000000000000000000000000000000000002") == {"AFTER_ADD_LIQUIDITY_RETURNS_DELTA"}


@pytest.mark.parametrize(
    "data,kind",
    [
        (bytes.fromhex("5bf6f916"), "expired"),
        (bytes.fromhex("6a12f104"), "slippage"),
        (bytes.fromhex("4713c18b") + bytes(64), "slippage"),
        (tc.selector("V3TooLittleReceived()"), "slippage"),
        (tc.selector("V4TooLittleReceived(uint256,uint256)") + bytes(64), "slippage"),
        (tc.selector("MinimumAmountInsufficient(uint128,uint128)") + bytes(64), "slippage"),
        (tc.selector("MaximumAmountExceeded(uint128,uint128)") + bytes(64), "slippage"),
        (tc.selector("InvalidSigner()"), "bad_signature"),
        (tc.selector("InvalidNonce()"), "bad_signature"),
        (tc.selector("InsufficientAllowance(uint256)") + bytes(32), "approve_pending"),
        (tc.selector("SliceOutOfBounds()"), "encoding"),
        (bytes.fromhex("383ef61c"), "encoding"),
        (b"", "unknown"),
        (bytes.fromhex("deadbeef"), "unknown"),
    ],
)
def test_decode_revert_selector_table(data, kind):
    assert decode_revert(data).kind == kind


def test_decode_revert_string_and_wrapped():
    from eth_abi import encode

    err = tc.selector("Error(string)") + encode(["string"], ["Price slippage check"])
    assert decode_revert(err) == tc.RevertKind("slippage", "0x08c379a0", "Price slippage check")
    old = tc.selector("Error(string)") + encode(["string"], ["Transaction too old"])
    assert decode_revert(old).kind == "expired"
    wrapped = tc.selector("WrappedError(address,bytes4,bytes,bytes)") + encode(
        ["address", "bytes4", "bytes", "bytes"], [PONS_HOOK, bytes.fromhex("259982e5"), b"\x01\x02", bytes.fromhex("a9e35b2f")]
    )
    decoded = decode_revert(wrapped)
    assert decoded.kind == "hook_reverted"
    assert decoded.detail.startswith(PONS_HOOK)
    failed = tc.selector("ExecutionFailed(uint256,bytes)") + encode(["uint256", "bytes"], [3, bytes.fromhex("5bf6f916")])
    assert decode_revert(failed).kind == "expired"
    assert decode_revert(failed).detail.startswith("command 3")
