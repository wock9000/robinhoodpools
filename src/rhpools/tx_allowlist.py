"""Pinned code hashes for every contract rhpools hands a user transaction to.

A target whose runtime code no longer hashes to the pinned value disables
trade and lp until the pin is reviewed. Hashes were read from the Nitro node on
2026-09-27 (keccak256 of eth_getCode at latest).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from eth_utils import keccak

from .tx_chain import NFPM_GIGA, NFPM_PANCAKE, NFPM_UNISWAP, PERMIT2, PONS_HOOK, POOL_MANAGER, POSM, UR

CODE_HASHES: dict[str, str] = {
    UR: "0x2ce6aaaf9f4151f5e1cbf774668772f17f532ae11b15e9284fd0a072a8b0fbde",
    PERMIT2: "0x5208783f52488f7d3493e5e38311ab707c1d75457fe472a19b0b4d57d66a7fca",
    POSM: "0xc873e135dc9aaec88489cfbad146b4cb49d6a32e0d80326377784b7ba17670b2",
    POOL_MANAGER: "0xbd3881180b547f5fe817545743cfb4343e96b1bc6640dcd70c106b0066e95626",
    NFPM_UNISWAP: "0x0a493d1af3d0f25fed8efa205244ebee14114267a08647fc38c515c7cd6ead4f",
    NFPM_PANCAKE: "0x1f78ea9b8894f894de8ed49ad312e37c1c15a4fbe5783bd41fb4a2e586d45f7f",
    NFPM_GIGA: "0x204c48c03559d3c265b26b7fec5859f0f95293052a1a1f461b944989fa234a2b",
    PONS_HOOK: "0xc21b1e6c1b45403e81a581f22ed6d9c747997af1cfdac1b1dc9f4b1d346a10db",
}


@dataclass(frozen=True)
class Mismatch:
    address: str
    expected: str
    actual: str


def code_hash(code_hex: Any) -> str:
    if not isinstance(code_hex, str) or not code_hex.startswith("0x"):
        raise RuntimeError("RPC returned malformed code")
    return "0x" + keccak(bytes.fromhex(code_hex[2:])).hex()


def verify(rpc: Any, block: str = "latest") -> tuple[Mismatch, ...]:
    """Return every pinned target whose code at ``block`` does not hash to its pin."""
    found: list[Mismatch] = []
    for address, expected in CODE_HASHES.items():
        actual = code_hash(rpc.call("eth_getCode", [address, block]))
        if actual != expected:
            found.append(Mismatch(address, expected, actual))
    return tuple(found)


__all__ = ["CODE_HASHES", "Mismatch", "code_hash", "verify"]
