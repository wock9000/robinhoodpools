"""Anvil fork harness with a stand-in ERC-20 for gate tests and the dev server."""
from __future__ import annotations

import json
import os
import socket
import subprocess
import time
from pathlib import Path
from urllib.request import Request, urlopen

from eth_keys import keys
from eth_utils import keccak, to_checksum_address

ANVIL = Path.home() / ".foundry/bin/anvil"
FORK_RPC = os.environ.get("RHP_TEST_FORK_RPC", "http://127.0.0.1:8547")
CHAIN_ID = 4663
STANDIN_TOKEN = to_checksum_address("0x" + "d00d" * 10)
STANDIN_ERC20 = (
    "0x60003560e01c806370a0823114601e578063313ce56714603857600080fd"
    "5b600435600052600060205260406000205460005260206000f3"
    "5b601260005260206000f3"
)


def rpc_call(url: str, method: str, params: list, timeout: float = 10.0):
    body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode()
    with urlopen(Request(url, data=body, headers={"Content-Type": "application/json"}), timeout=timeout) as response:
        reply = json.loads(response.read())
    if "error" in reply:
        raise ValueError(reply["error"])
    return reply["result"]


def upstream_available() -> bool:
    try:
        return rpc_call(FORK_RPC, "eth_chainId", [], timeout=3.0) == hex(CHAIN_ID)
    except (OSError, ValueError):
        return False


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def fresh_key(seed: bytes | None = None) -> keys.PrivateKey:
    return keys.PrivateKey(seed or os.urandom(32))


def balance_slot(wallet: str) -> str:
    return "0x" + keccak(bytes.fromhex(wallet[2:]).rjust(32, b"\0") + b"\0" * 32).hex()


class Fork:
    def __init__(self) -> None:
        self.port = free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.process: subprocess.Popen | None = None

    def start(self) -> "Fork":
        self.process = subprocess.Popen(
            [str(ANVIL), "--fork-url", FORK_RPC, "--port", str(self.port), "--chain-id", str(CHAIN_ID), "--silent"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                if rpc_call(self.url, "eth_chainId", [], timeout=2.0) == hex(CHAIN_ID):
                    break
            except (OSError, ValueError):
                time.sleep(0.2)
        else:
            self.stop()
            raise RuntimeError("anvil did not come up")
        rpc_call(self.url, "anvil_setCode", [STANDIN_TOKEN, STANDIN_ERC20])
        return self

    def stop(self) -> None:
        if self.process is not None:
            self.process.kill()
            self.process.wait(timeout=10)
            self.process = None

    def rpc(self, method: str, params: list):
        return rpc_call(self.url, method, params)

    def set_balance(self, wallet: str, amount: int) -> None:
        self.rpc("anvil_setStorageAt", [STANDIN_TOKEN, balance_slot(wallet), "0x" + f"{amount:064x}"])
        self.rpc("anvil_mine", ["0x1"])

    def fund_gas(self, wallet: str, wei: int = 10**18) -> None:
        self.rpc("anvil_setBalance", [wallet, hex(wei)])

    def balance_of(self, wallet: str) -> int:
        data = "0x70a08231" + wallet[2:].lower().rjust(64, "0")
        return int(self.rpc("eth_call", [{"to": STANDIN_TOKEN, "data": data}, "latest"]), 16)

    def __enter__(self) -> "Fork":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop()
