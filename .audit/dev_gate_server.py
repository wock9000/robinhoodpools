"""Loopback dev server: terminal + real gate on an anvil fork with the stand-in token.

    .venv/bin/python .audit/dev_gate_server.py [--port 8297]
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "tests"), str(ROOT / "src")]

from eth_utils import keccak  # noqa: E402

from gate_fork import CHAIN_ID, STANDIN_TOKEN, Fork, balance_slot, fresh_key  # noqa: E402
from golden.anonymous import stub_runtime  # noqa: E402
from rhpools.lp_gate import Gate, GatePolicy, Limits  # noqa: E402
from rhpools.lp_server import Handler, LPHTTPServer, _load_assets  # noqa: E402
from rhpools.tx_core import JsonRpc, TxCore  # noqa: E402
from rhpools.tx_plan import TxPolicy  # noqa: E402
from rhpools.tx_routes import RouteBook  # noqa: E402
import contextlib  # noqa: E402
import sqlite3  # noqa: E402

MARKET_DB = '/home/andnasnd/.local/share/rhpools-nocow/lp_market.sqlite'


@contextlib.contextmanager
def market_pools():
    connection = sqlite3.connect(f'file:{MARKET_DB}?mode=ro', uri=True, timeout=5)
    try:
        yield connection
    finally:
        connection.close()
from test_lp_market_service import header, lp_effect, pools, position_state, service  # noqa: E402

DECIMALS = 18
THRESHOLD = 1000 * 10**DECIMALS


def market(tmp: Path):
    app = service(tmp / "market.sqlite")
    app.store.upsert_pools(pools())
    block = header(100, int(time.time()))
    event = lp_effect(block, "v4", "add", 1000, (1000000, 1000000), position_state(0), position_state(1000))
    event["pool"] = pools()[1]
    app.indexer._decode_current = lambda *a, **k: [event]
    app.indexer.feed_updates()
    app.indexer._publish_current_block(block, [], source="dev")
    return app


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8297)
    args = ap.parse_args()
    owner, holder, poor = (fresh_key(keccak(text=f"rhpools dev {name}")) for name in ("owner", "holder", "nobody"))
    tmp = Path(tempfile.mkdtemp(prefix="rhp-gate-dev-"))
    fork = Fork().start()
    try:
        for key in (owner, holder, poor):
            fork.fund_gas(key.public_key.to_checksum_address())
        fork.set_balance(holder.public_key.to_checksum_address(), 5000 * 10**DECIMALS)
        fork.fund_gas(holder.public_key.to_checksum_address(), 20 * 10**18)
        gate = Gate(
            tmp / "gate.sqlite", owner=owner.public_key.to_checksum_address(), rpc_url=fork.url,
            hosts=frozenset({f"localhost:{args.port}", f"127.0.0.1:{args.port}"}), limits=Limits(),
        )
        policy = GatePolicy.parse({
            "version": 1, "token": STANDIN_TOKEN, "decimals": DECIMALS,
            "threshold": {"trade": str(THRESHOLD), "lp": str(THRESHOLD), "api": str(THRESHOLD), "flags": "0"},
            "grace_s": 120, "issued_at": int(time.time()),
        })
        if not os.environ.get("RHP_DEV_NO_POLICY"):
            gate.apply_policy(policy, owner.sign_msg_hash(policy.digest(CHAIN_ID)).to_hex(), via="cli")
        app = market(tmp)
        runtime = stub_runtime()
        runtime.lp = app
        runtime.gate = gate
        fee_to = fresh_key(keccak(text='rhpools dev fee')).public_key.to_checksum_address().lower()
        tx_rpc = JsonRpc(fork.url)
        runtime.tx = TxCore(tx_rpc, RouteBook(market_pools, tx_rpc), TxPolicy(75, fee_to))
        runtime.tx_unavailable = None if runtime.tx.enabled else 'allowlist mismatch'
        runtime.assets = _load_assets()
        runtime.origins = frozenset({f"http://localhost:{args.port}", f"http://127.0.0.1:{args.port}"})
        handler = type("DevHandler", (Handler,), {"runtime": runtime})
        server = LPHTTPServer(("127.0.0.1", args.port), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        print(f"terminal        http://localhost:{args.port}/   (use localhost, not 127.0.0.1: the __Host- cookie needs a secure context)")
        print(f"anvil fork rpc  {fork.url}   (chain {CHAIN_ID}; add as a network in the wallet if you want balances shown)")
        print(f"token           {STANDIN_TOKEN}   threshold 1000, grace 120 s, decimals {DECIMALS}")
        print(f"holder  {holder.public_key.to_checksum_address()}  pk {holder.to_hex()}   (5000 tokens)")
        print(f"nobody  {poor.public_key.to_checksum_address()}  pk {poor.to_hex()}   (0 tokens)")
        print(f"owner   {owner.public_key.to_checksum_address()}  pk {owner.to_hex()}   (policy signer, 0 tokens)")
        print(f"gate db {tmp / 'gate.sqlite'}")
        print(f"tx core enabled={runtime.tx.enabled} fee recipient {fee_to} (75 bps); holder has 20 ETH on the fork")
        slot = balance_slot(holder.public_key.to_checksum_address())
        print("drain the holder:  curl -s -X POST " + fork.url + " -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"anvil_setStorageAt\",\"params\":[\"" + STANDIN_TOKEN + "\",\"" + slot + "\",\"0x" + "00" * 32 + "\"]}'")
        print("refund the holder: same call with \"0x" + f"{5000 * 10**DECIMALS:064x}" + "\"; the oracle re-reads within 30 s")
        print("ctrl-c stops the server and kills anvil", flush=True)
        stopping = threading.Event()
        signal.signal(signal.SIGINT, lambda *a: stopping.set())
        signal.signal(signal.SIGTERM, lambda *a: stopping.set())
        stopping.wait()
        runtime.stopping.set()
        server.shutdown()
        server.server_close()
        app.close()
        gate.close()
    finally:
        fork.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
