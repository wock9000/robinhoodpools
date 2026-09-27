"""Measure flow tags against the chain, or capture test fixtures from it.

    python tools/flow_tags_sample.py measure [--rpc URL] [--market PATH] [--swaps 200]
    python tools/flow_tags_sample.py capture [--rpc URL] [--market PATH] [--out DIR]

``measure`` classifies the most recent swaps, checks every PONS tag against a
direct ``launches(poolId)`` read, and, when ``RHP_LISTENER_DSN`` is set, reports
on-chain versus listener FOMO agreement with each disagreement listed. The market
database is opened read-only; the tag store is a temporary file.
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
import tempfile
from collections import Counter
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from rhpools import lp_flow_tags as tags  # noqa: E402
from rhpools.lp_chain import POOL_MANAGER  # noqa: E402

SWAP_V4 = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
SWAP_V3 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
DEFAULT_MARKET = os.path.expanduser("~/.local/share/rhpools-nocow/lp_market.sqlite")
FOMO_WALLET_CODE = "0xef0100e6cae83bde06e4c305530e199d7217f42808555b"
ENTRYPOINT = "0x4337084d9e255ff0702461cf8895ce9e3b5ff108"
MULTICALL3 = "0xca11bde05977b3631167028862be2a173976ca11"


class HttpRpc:
    def __init__(self, url: str) -> None:
        self.url = url
        self.session = requests.Session()

    def call(self, method: str, params: list[Any]) -> Any:
        body = self.session.post(
            self.url, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=120,
        ).json()
        if "error" in body:
            raise RuntimeError(body["error"])
        return body["result"]

    def batch(self, calls: Any, size: int = 200) -> list[Any]:
        calls = list(calls)
        results: list[Any] = []
        for start in range(0, len(calls), size):
            chunk = calls[start:start + size]
            payload = [
                {"jsonrpc": "2.0", "id": index, "method": method, "params": list(params)}
                for index, (method, params) in enumerate(chunk)
            ]
            body = self.session.post(self.url, json=payload, timeout=120).json()
            if isinstance(body, dict):
                raise RuntimeError(body)
            body.sort(key=lambda item: item["id"])
            for item in body:
                if "error" in item:
                    raise RuntimeError(item["error"])
                results.append(item["result"])
        return results


def market(path: str) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def recent_swaps(
    rpc: HttpRpc, db: sqlite3.Connection, count: int, head_offset: int = 0,
) -> list[dict[str, Any]]:
    head = int(rpc.call("eth_blockNumber", []), 16) - head_offset
    span = 200
    while True:
        window = {"fromBlock": hex(head - span), "toBlock": hex(head)}
        logs = rpc.call("eth_getLogs", [{**window, "address": POOL_MANAGER, "topics": [SWAP_V4]}])
        logs += rpc.call("eth_getLogs", [{**window, "topics": [SWAP_V3]}])
        rows = []
        for log in logs:
            if log["topics"][0] == SWAP_V4:
                pool_id = log["topics"][1].lower()
            else:
                pool_id = log["address"].lower()
            pool = db.execute(
                "SELECT id,protocol,hook FROM pools WHERE id=?", (pool_id,),
            ).fetchone()
            if pool is None:
                continue
            rows.append({
                "tx_hash": log["transactionHash"].lower(),
                "block_number": int(log["blockNumber"], 16),
                "log_index": int(log["logIndex"], 16),
                "pool_id": pool_id,
                "pool": dict(pool),
            })
        rows.sort(key=lambda row: (row["block_number"], row["log_index"]))
        if len(rows) >= count or span >= 20000:
            break
        span *= 4
    rows = rows[-count:]
    blocks = sorted({row["block_number"] for row in rows})
    headers = rpc.batch([("eth_getBlockByNumber", [hex(number), False]) for number in blocks])
    times = {number: int(header["timestamp"], 16) for number, header in zip(blocks, headers)}
    for row in rows:
        row["timestamp"] = times[row["block_number"]]
    return rows


def launches_word0(rpc: HttpRpc, pool_ids: list[str]) -> dict[str, bool]:
    results = rpc.batch([
        ("eth_call", [{"to": tags.PONS_HOOK, "data": tags.LAUNCHES_SELECTOR + pool_id[2:]}, "latest"])
        for pool_id in pool_ids
    ])
    return {pool_id: int(result[2:66], 16) != 0 for pool_id, result in zip(pool_ids, results)}


def fill_recipient_codes(rpc: HttpRpc, hashes: list[str]) -> Counter:
    receipts = rpc.batch([("eth_getTransactionReceipt", [tx_hash]) for tx_hash in hashes])
    recipients: dict[str, str | None] = {}
    for tx_hash, receipt in zip(hashes, receipts):
        last = None
        for log in receipt["logs"]:
            topics = log["topics"]
            if len(topics) == 3 and topics[0] == tags.TOPIC_TRANSFER and topics[1] == tags.EXECUTOR_TOPIC:
                last = "0x" + topics[2][-40:].lower()
        recipients[tx_hash] = last
    wallets = sorted({wallet for wallet in recipients.values() if wallet})
    codes = dict(zip(wallets, rpc.batch([("eth_getCode", [wallet, "latest"]) for wallet in wallets])))
    outcome: Counter = Counter()
    for wallet in recipients.values():
        if wallet is None:
            outcome["no_executor_transfer"] += 1
        elif codes[wallet].lower() == FOMO_WALLET_CODE:
            outcome["fomo_wallet_code"] += 1
        elif codes[wallet] == "0x":
            outcome["plain_eoa"] += 1
        else:
            outcome["other_code"] += 1
    return outcome


def measure(args: argparse.Namespace) -> int:
    rpc = HttpRpc(args.rpc)
    db = market(args.market)
    listener = tags.listener_from_env()
    head_offset = args.head_offset
    if isinstance(listener, tags.PostgresListener):
        listener = tags.PostgresListener(
            os.environ["RHP_LISTENER_DSN"], window_s=args.listener_window,
            tail_budget=args.listener_budget,
        )
        if listener.refresh() and listener.state()["latest_block"]:
            head = int(rpc.call("eth_blockNumber", []), 16)
            head_offset = max(head_offset, head - listener.state()["latest_block"] + 50)
            print(f"listener covers up to block {listener.state()['latest_block']}, "
                  f"head {head}, sampling {head_offset} blocks below head")
    rows = recent_swaps(rpc, db, args.swaps, head_offset)
    pools = {row["pool_id"]: row["pool"] for row in rows}
    with tempfile.TemporaryDirectory() as directory:
        store = tags.TagStore(os.path.join(directory, "tags.sqlite"))
        tagger = tags.FlowTagger(rpc, store, pools.get, listener)
        result = tagger.tag(rows)
        status = tagger.status()
        store.close()
    by_key = {(tag.tx_hash, tag.pool_id): tag for tag in result}
    print(f"sampled {len(rows)} swaps over blocks "
          f"{rows[0]['block_number']}..{rows[-1]['block_number']}, tagged {len(result)}")
    print("chain basis:", dict(Counter(
        " ".join(sorted(t.basis & tags.CHAIN_FOMO_BASIS)) or "-" for t in result
    )))
    print("tags:", dict(Counter(" ".join(sorted(t.tags)) or "-" for t in result)))

    v4_pools = sorted({row["pool_id"] for row in rows if row["pool"]["protocol"] == "v4"})
    oracle = launches_word0(rpc, v4_pools)
    pons_mismatch = [
        (key, tag) for key, tag in by_key.items()
        if (tags.PONS in tag.tags) != oracle.get(key[1], False)
    ]
    print(f"PONS exactness: {len(by_key) - len(pons_mismatch)}/{len(by_key)} (tx, pool) pairs match "
          f"launches(poolId) directly; {sum(oracle.values())}/{len(v4_pools)} V4 pools registered")
    for (tx_hash, pool_id), tag in pons_mismatch:
        print("  mismatch", tx_hash, pool_id, sorted(tag.basis), pools[pool_id]["hook"])

    fill_hashes = sorted({t.tx_hash for t in result if tags.BASIS_FOMO_FILL in t.basis})
    if fill_hashes:
        print("executor fill recipients:", dict(fill_recipient_codes(rpc, fill_hashes)))

    print("listener:", json.dumps(status["listener"]))
    agreement = status["fomo_agreement"]
    print("FOMO agreement:", json.dumps(agreement))
    if agreement["compared"]:
        seen = set()
        for tag in result:
            if tag.tx_hash in seen or tag.chain_fomo == tag.listener_fomo:
                continue
            seen.add(tag.tx_hash)
            env_to = next(row for row in rows if row["tx_hash"] == tag.tx_hash)
            print("  disagreement", tag.tx_hash, "block", env_to["block_number"],
                  "basis", sorted(tag.basis))
        early = sorted(t.early_ms for t in result if t.early_ms is not None)
        if early:
            print(f"early_ms: {len(early)} FOMO rows, min {early[0]} median "
                  f"{early[len(early) // 2]} max {early[-1]}")
    if hasattr(listener, "close"):
        listener.close()
    return 1 if pons_mismatch else 0


def capture(args: argparse.Namespace) -> int:
    rpc = HttpRpc(args.rpc)
    db = market(args.market)
    rows = recent_swaps(rpc, db, 2000)
    hashes = list(dict.fromkeys(row["tx_hash"] for row in rows))
    transactions = dict(zip(hashes, rpc.batch([("eth_getTransactionByHash", [h]) for h in hashes])))
    footprint: dict[str, list[dict[str, Any]]] = {h: [] for h in hashes}
    blocks = [row["block_number"] for row in rows]
    for batch in rpc.batch(tags.footprint_filters(min(blocks), max(blocks))):
        for log in batch:
            if log["transactionHash"].lower() in footprint:
                footprint[log["transactionHash"].lower()].append(log)
    v4_pools = sorted({row["pool_id"] for row in rows if row["pool"]["protocol"] == "v4"})
    registered = launches_word0(rpc, v4_pools)

    def pick(name: str, predicate: Any) -> dict[str, Any] | None:
        for row in reversed(rows):
            tx = transactions[row["tx_hash"]]
            to = (tx.get("to") or "").lower()
            pons = registered.get(row["pool_id"], False)
            deposits, fill = tags.relay_footprint(footprint[row["tx_hash"]])
            if predicate(to, pons, row, bool(deposits), fill):
                return {
                    "name": name, "tx": tx, "footprint_logs": footprint[row["tx_hash"]],
                    "block_time": row["timestamp"], "pool": row["pool"],
                    "launches_word0_nonzero": pons,
                }
        return None

    wanted = {
        "router_pons": lambda to, pons, row, dep, fill: to == tags.FOMO_ROUTER and pons and fill and not dep,
        "router_plain": lambda to, pons, row, dep, fill: to == tags.FOMO_ROUTER and not pons and fill and not dep,
        "executor_pons": lambda to, pons, row, dep, fill: to == tags.EXECUTOR and pons and fill and not dep,
        "entrypoint_sell": lambda to, pons, row, dep, fill: to == ENTRYPOINT and dep and fill,
        "multicall_sell": lambda to, pons, row, dep, fill: to == MULTICALL3 and dep,
        "direct_pons": lambda to, pons, row, dep, fill: pons and not dep and not fill,
        "direct_v3": lambda to, pons, row, dep, fill: row["pool"]["protocol"] == "v3" and not dep and not fill,
    }
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for name, predicate in wanted.items():
        fixture = pick(name, predicate)
        if fixture is None:
            print("no sample for", name)
            continue
        (out / f"{name}.json").write_text(json.dumps(fixture, indent=1, sort_keys=True) + "\n")
        print("wrote", name, fixture["tx"]["hash"])
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("measure", "capture"))
    parser.add_argument("--rpc", default="http://127.0.0.1:8547")
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--swaps", type=int, default=200)
    parser.add_argument("--head-offset", type=int, default=0,
                        help="extra blocks below head to sample from")
    parser.add_argument("--listener-window", type=int, default=900)
    parser.add_argument("--listener-budget", type=int, default=400000)
    parser.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "flow_tags"))
    args = parser.parse_args(argv)
    return measure(args) if args.command == "measure" else capture(args)


if __name__ == "__main__":
    sys.exit(main())
