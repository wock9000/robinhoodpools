"""_mc.py — minimal Multicall3 batching for the hot polling loops.

Measured 2026-08-21: one client was issuing 109 eth_calls/second — 93% of
all traffic to the local Nitro node — because `memewatch tail` reads
slot0 + liquidity for every touched pair SERIALLY on a 2s cadence. The
node answers each in ~1.3ms, so nothing looked broken; the cost is
volume, and volume is what competes with the fire paths of every other
lane.

Multicall3.aggregate3 collapses N reads into one round trip AND one block
state root, so a batch cannot straddle a block boundary and mix states —
which matters at 10 blocks/s.

Deliberately tiny and dependency-light so any lane can import it without
dragging in accounting/lpstate. `accounting.py` keeps its own copy: it
also stamps the L2 height via ArbSys inside the same aggregate, and that
path is load-bearing for the portfolio numbers.
"""
from __future__ import annotations

MULTICALL3 = "0xcA11bde05977b3631167028862bE2a173976CA11"
_AGG3 = "0x82ad56cb"          # aggregate3((address,bool,bytes)[])
MAX_PER_BATCH = 240           # sub-calls per round trip; keeps payloads sane


def encode(calls: list[tuple[str, str]]) -> str:
    """[(target, calldata_hex)] -> aggregate3 calldata."""
    from eth_abi import encode as abi_encode
    from eth_utils import to_checksum_address
    packed = [(to_checksum_address(t), True, bytes.fromhex(d[2:]))
              for t, d in calls]
    return _AGG3 + abi_encode(["(address,bool,bytes)[]"], [packed]).hex()


def decode(result_hex: str) -> list[tuple[bool, bytes]]:
    from eth_abi import decode as abi_decode
    return abi_decode(["(bool,bytes)[]"],
                      bytes.fromhex(result_hex[2:]))[0]


def batch_with_status(
    rpc_call,
    calls: list[tuple[str, str]],
) -> tuple[list[tuple[bool, bytes]], bool]:
    """Run aggregate3 calls and report whether every transport round trip decoded."""
    out: list[tuple[bool, bytes]] = []
    transport_ok = True
    for i in range(0, len(calls), MAX_PER_BATCH):
        chunk = calls[i:i + MAX_PER_BATCH]
        res = rpc_call(MULTICALL3, encode(chunk))
        if not res or len(res) < 4:
            out += [(False, b"")] * len(chunk)
            transport_ok = False
            continue
        try:
            decoded = decode(res)
        except Exception:
            out += [(False, b"")] * len(chunk)
            transport_ok = False
            continue
        if len(decoded) != len(chunk):
            out += [(False, b"")] * len(chunk)
            transport_ok = False
            continue
        out += list(decoded)
    return out, transport_ok


def batch(rpc_call, calls: list[tuple[str, str]]) -> list[tuple[bool, bytes]]:
    """Run calls through aggregate3 with per-subcall allowFailure."""
    return batch_with_status(rpc_call, calls)[0]
