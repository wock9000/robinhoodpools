"""Measure flow tags against the chain, or capture test fixtures from it.

    python tools/flow_tags_sample.py measure [--rpc URL] [--market PATH] [--swaps 200]
    python tools/flow_tags_sample.py evidence [--swaps 1000] [--reported-minutes 60]
    python tools/flow_tags_sample.py capture [--rpc URL] [--market PATH] [--out DIR]

``measure`` classifies the most recent swaps, checks every PONS tag against a
direct ``launches(poolId)`` read, and, when ``RHP_LISTENER_DSN`` is set, reports
on-chain versus listener FOMO agreement with each disagreement listed.

``evidence`` needs ``RHP_LISTENER_DSN`` (read-only) and checks the FOMO tag
against FOMO's own wallet roster in apollo (fomo_wallet_bindings, official
trader candidates, curated wallets) and against the FOMO fee-receiver trade
feed (fomo_public_trade_observation) linked through relay-listener order ids.

The market database is opened read-only; the tag store is a temporary file.
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
    print("basis:", dict(Counter(" ".join(sorted(t.basis - {tags.BASIS_PONS_HOOK})) or "-" for t in result)))
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

    print("wallet codes:", json.dumps(status["wallet_codes"]), "basis totals:", json.dumps(status["basis"]))

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
    envelopes = tags.fetch_envelopes(rpc, [(row["tx_hash"], row["block_number"], row["timestamp"]) for row in rows])
    logs: dict[str, list[dict[str, Any]]] = {h: [] for h in hashes}
    blocks = [row["block_number"] for row in rows]
    for batch in rpc.batch(tags.footprint_filters(min(blocks), max(blocks))):
        for log in batch:
            if log["transactionHash"].lower() in logs:
                logs[log["transactionHash"].lower()].append(log)
    wallets = sorted({wallet for env in envelopes.values() for wallet in env.wallets})
    codes = dict(zip(wallets, rpc.batch([("eth_getCode", [wallet, "latest"]) for wallet in wallets])))
    fomo_wallets = {wallet for wallet, code in codes.items() if code.lower() == tags.FOMO_WALLET_CODE}
    transactions = dict(zip(hashes, rpc.batch([("eth_getTransactionByHash", [h]) for h in hashes])))
    v4_pools = sorted({row["pool_id"] for row in rows if row["pool"]["protocol"] == "v4"})
    registered = launches_word0(rpc, v4_pools)

    def pick(name: str, predicate: Any) -> dict[str, Any] | None:
        for row in reversed(rows):
            env = envelopes.get(row["tx_hash"])
            if env is None:
                continue
            pons = registered.get(row["pool_id"], False)
            fomo = bool(fomo_wallets & set(env.wallets))
            if predicate(env, pons, row, fomo):
                return {
                    "name": name, "tx": transactions[row["tx_hash"]], "footprint_logs": logs[row["tx_hash"]],
                    "wallet_codes": {wallet: codes[wallet] for wallet in env.wallets},
                    "block_time": row["timestamp"], "pool": row["pool"],
                    "launches_word0_nonzero": pons,
                }
        return None

    def fill_only(env: tags.TxEnvelope) -> bool:
        return bool(env.fill_recipients) and not env.deposit_wallets

    wanted = {
        "router_pons": lambda env, pons, row, fomo: env.to == tags.FOMO_ROUTER and pons and fill_only(env) and fomo,
        "router_plain": lambda env, pons, row, fomo: env.to == tags.FOMO_ROUTER and not pons and fill_only(env) and fomo,
        "router_eoa_fill": lambda env, pons, row, fomo: env.to == tags.FOMO_ROUTER and fill_only(env) and not fomo,
        "entrypoint_sell": lambda env, pons, row, fomo: env.to == tags.ENTRYPOINT and env.deposit_wallets
        and env.zero_gas_senders and fomo,
        "multicall_sell": lambda env, pons, row, fomo: env.to == MULTICALL3 and env.deposit_wallets and not fomo,
        "direct_pons": lambda env, pons, row, fomo: pons and not env.relay_footprint,
        "direct_v3": lambda env, pons, row, fomo: row["pool"]["protocol"] == "v3" and not env.relay_footprint,
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


ROSTER_SQL = """
    SELECT DISTINCT encode(address, 'hex') FROM (
        SELECT w.address FROM fomo_wallet_bindings b JOIN wallets w ON w.id = b.wallet_id
        WHERE b.chain_id = 50 AND w.address = ANY(%(wallets)s)
        UNION ALL
        SELECT evm_wallet FROM fomo_official_trader_candidate WHERE evm_wallet = ANY(%(wallets)s)
        UNION ALL
        SELECT decode(substr(evm_wallet, 3), 'hex') FROM fomo_curated_trader_wallet
        WHERE decode(substr(evm_wallet, 3), 'hex') = ANY(%(wallets)s)
    ) roster
"""


def pg_connect() -> Any:
    import psycopg

    connection = psycopg.connect(
        os.environ["RHP_LISTENER_DSN"], connect_timeout=5, application_name="rhpools-flow-tags-evidence",
        options="-c default_transaction_read_only=on -c statement_timeout=120000",
    )
    connection.read_only = True
    return connection


def roster_members(connection: Any, wallets: set[str]) -> set[str]:
    if not wallets:
        return set()
    raw = [bytes.fromhex(wallet[2:]) for wallet in sorted(wallets)]
    with connection.transaction():
        rows = connection.execute(ROSTER_SQL, {"wallets": raw}).fetchall()
    return {"0x" + row[0] for row in rows}


def chunked_logs(rpc: HttpRpc, low: int, high: int, filters: Any, step: int = 2000) -> list[dict[str, Any]]:
    calls = []
    for start in range(low, high + 1, step):
        calls.extend(filters(start, min(start + step - 1, high)))
    return [log for batch in rpc.batch(calls, size=20) for log in batch]


def wallet_classes(rpc: HttpRpc, wallets: set[str], roster: set[str]) -> dict[str, str]:
    ordered = sorted(wallets)
    codes = dict(zip(ordered, rpc.batch([("eth_getCode", [wallet, "latest"]) for wallet in ordered])))
    classes = {}
    for wallet in ordered:
        code = codes[wallet].lower()
        if wallet in roster:
            classes[wallet] = "roster"
        elif code == tags.FOMO_WALLET_CODE:
            classes[wallet] = "fomo_code"
        elif code.startswith("0xef0100"):
            classes[wallet] = "other_7702"
        elif code == "0x":
            classes[wallet] = "eoa"
        else:
            classes[wallet] = "contract"
    return classes


def ledger_window(connection: Any, start_ms: int, end_ms: int) -> list[tuple[Any, ...]]:
    """Ledger rows with event time inside [start_ms, end_ms], read along the primary key."""

    def query(sql: str, params: tuple[Any, ...]) -> list[Any]:
        with connection.transaction():
            return connection.execute(sql, params).fetchall()

    rows: list[tuple[Any, ...]] = []
    instances = query(
        "SELECT source_instance_id, last_sequence FROM relay_listener_research_source_state "
        "WHERE last_event_at_unix_ms >= %s", (start_ms,),
    )
    for instance, last_sequence in instances:
        first = query(
            "SELECT event_at_unix_ms FROM relay_listener_research_event "
            "WHERE source_instance_id = %s ORDER BY sequence LIMIT 1", (instance,),
        )
        if not first or int(first[0][0]) > end_ms:
            continue
        low, high = 0, int(last_sequence)
        while low < high:
            middle = (low + high + 1) // 2
            probe = query(
                "SELECT event_at_unix_ms FROM relay_listener_research_event "
                "WHERE source_instance_id = %s AND sequence >= %s ORDER BY sequence LIMIT 1",
                (instance, middle),
            )
            if probe and int(probe[0][0]) < start_ms:
                low = middle
            else:
                high = middle - 1
        cursor = low
        while True:
            page = query(
                "SELECT sequence, event_at_unix_ms, order_id, "
                "payload->'observation'->>'chain', payload->'observation'->>'observation_kind', "
                "payload->'observation'->>'transaction_hash', payload->'observation'->>'wallet', "
                "payload->'observation'->>'block_number', payload->'observation'->>'status' "
                "FROM relay_listener_research_event WHERE source_instance_id = %s AND sequence > %s "
                "AND record_kind = 'fomo_observation' ORDER BY sequence LIMIT 5000",
                (instance, cursor),
            )
            if not page:
                break
            cursor = int(page[-1][0])
            rows.extend(row for row in page if int(row[1]) <= end_ms)
            if int(page[-1][1]) > end_ms:
                break
    return rows


def classify_hashes(rpc: HttpRpc, hashes: list[str]) -> dict[str, tags.FlowTag]:
    transactions = rpc.batch([("eth_getTransactionByHash", [h]) for h in hashes])
    requests = [(h, int(tx["blockNumber"], 16), 0) for h, tx in zip(hashes, transactions) if tx]
    with tempfile.TemporaryDirectory() as directory:
        store = tags.TagStore(os.path.join(directory, "tags.sqlite"))
        envelopes = tags.fetch_envelopes(rpc, requests)
        fomo_wallets = tags.WalletCodes(rpc, store).fomo(w for env in envelopes.values() for w in env.wallets)
        pool = tags.PoolIdentity("0x", "", None, False)
        result = {h: tags.classify(pool, env, None, fomo_wallets) for h, env in envelopes.items()}
        store.close()
    return result


def evidence(args: argparse.Namespace) -> int:
    rpc = HttpRpc(args.rpc)
    db = market(args.market)
    connection = pg_connect()
    rows = recent_swaps(rpc, db, args.swaps, args.head_offset)
    hashes = list(dict.fromkeys(row["tx_hash"] for row in rows))
    low, high = rows[0]["block_number"], rows[-1]["block_number"]
    transactions = dict(zip(hashes, rpc.batch([("eth_getTransactionByHash", [h]) for h in hashes])))
    logs: dict[str, list[dict[str, Any]]] = {h: [] for h in hashes}
    for log in chunked_logs(rpc, low, high, tags.footprint_filters):
        if log["transactionHash"].lower() in logs:
            logs[log["transactionHash"].lower()].append(log)
    envelopes = {h: tags.envelope(transactions[h], 0, logs[h]) for h in hashes}
    senders: dict[str, set[str]] = {h: {transactions[h]["from"].lower()} for h in hashes}
    for log in (log for h in hashes for log in logs[h]):
        if log["address"].lower() == tags.ENTRYPOINT and log["topics"][0].lower() == tags.TOPIC_USER_OPERATION:
            senders[log["transactionHash"].lower()].add("0x" + log["topics"][2][-40:].lower())
    counterparties = {h: set(env.deposit_wallets) | set(env.fill_recipients) for h, env in envelopes.items()}
    every_wallet = set().union(*senders.values(), *counterparties.values())
    roster = roster_members(connection, every_wallet)
    classes = wallet_classes(rpc, every_wallet, roster)
    fomo_wallets = frozenset(w for w, kind in classes.items() if kind == "fomo_code")
    pool = tags.PoolIdentity("0x", "", None, False)
    tagged = {h: tags.classify(pool, env, None, fomo_wallets) for h, env in envelopes.items()}
    relay = {h for h, env in envelopes.items() if env.relay_footprint}
    fomo = {h for h, tag in tagged.items() if tags.FOMO in tag.tags}
    print(f"sampled {len(rows)} swaps in {len(hashes)} transactions over blocks {low}..{high}; "
          f"{len(relay)} Relay-footprint, {len(fomo)} tagged FOMO; roster hits {len(roster)} of {len(every_wallet)} wallets")
    print("basis:", dict(Counter(" ".join(sorted(tag.basis)) or "-" for tag in tagged.values())))

    rank = {"roster": 0, "fomo_code": 1, "other_7702": 2, "eoa": 3, "contract": 4}
    best = {
        h: min((classes[w] for w in counterparties[h]), key=rank.get) if counterparties[h] else "no_wallet"
        for h in relay
    }
    print("Relay-footprint transactions by best counterparty wallet class:", dict(Counter(best.values())))
    sells = {h for h in relay if envelopes[h].deposit_wallets}
    print(f"  sells ({len(sells)}) by depositor class:", dict(Counter(
        min((classes[w] for w in envelopes[h].deposit_wallets), key=rank.get) for h in sells
    )))
    print(f"  fills ({len(relay - sells)}) by best recipient class:", dict(Counter(best[h] for h in relay - sells)))
    confirmed = {h for h in fomo if best[h] == "roster"}
    print(f"precision vs FOMO-code identity: {len(fomo)}/{len(fomo)} = 1.000 by construction; "
          f"roster-confirmed {len(confirmed)}/{len(fomo)}; generic Relay trades excluded {len(relay - fomo)}/{len(relay)}")
    for h in sorted(relay - fomo):
        env = envelopes[h]
        print("  excluded relay trade", h, "to", env.to, best[h],
              "sell" if h in sells else "fill", sorted(counterparties[h])[:2])
    zero_gas = {h for h, env in envelopes.items() if env.zero_gas_senders}
    gas_by_class = Counter(
        (min((classes[w] for w in senders[h]), key=rank.get), "zero_gas" if h in zero_gas else "paid_or_none")
        for h in hashes if len(senders[h]) > 1
    )
    print("user-op transactions by sender class and gas:", dict(gas_by_class))
    print(f"  zero-gas user-op transactions tagged FOMO: {len(zero_gas & fomo)}/{len(zero_gas)}")

    roster_txs = {h for h in hashes if any(classes[w] == "roster" for w in senders[h] | counterparties[h])}
    print(f"reverse: {len(roster_txs)} sampled transactions involve a rostered FOMO wallet; "
          f"{len(roster_txs & fomo)} tagged FOMO")
    for h in sorted(roster_txs - fomo):
        print("  rostered wallet without FOMO tag", h, "to", envelopes[h].to)
    code_senders = {h for h in hashes if any(classes[w] == "fomo_code" for w in senders[h])}
    print(f"{len(code_senders & fomo)}/{len(code_senders)} transactions with a FOMO-code sender are tagged FOMO")

    reported_end = connection.execute(
        "SELECT max(observed_at) FROM fomo_public_trade_observation"
    ).fetchone()[0]
    if reported_end is None:
        print("no fomo_public_trade_observation rows")
        return 0
    end_ms = int(reported_end.timestamp() * 1000)
    start_ms = end_ms - args.reported_minutes * 60_000
    with connection.transaction():
        reported = connection.execute(
            "SELECT signature FROM fomo_public_trade_observation "
            "WHERE occurred_at >= to_timestamp(%s) AND occurred_at <= to_timestamp(%s)",
            (start_ms / 1000, end_ms / 1000),
        ).fetchall()
    signatures = {row[0] for row in reported}
    ledger = ledger_window(connection, start_ms - 60_000, end_ms + 600_000)
    orders = {row[2] for row in ledger if row[3] == "solana" and row[4] == "payment" and row[5] in signatures and row[2]}
    fills = {row[5].lower() for row in ledger if row[3] == "robinhood" and row[2] in orders and row[8] == "observed"}
    print(f"FOMO fee-receiver trades {reported_end.isoformat()} minus {args.reported_minutes} min: "
          f"{len(signatures)} signatures, {len(orders)} matched Relay orders, "
          f"{len(fills)} Robinhood fill transactions in the ledger ({len(ledger)} ledger rows scanned)")
    if fills:
        fill_tags = classify_hashes(rpc, sorted(fills))
        tagged_fills = [h for h, tag in fill_tags.items() if tags.FOMO in tag.tags]
        print(f"  {len(tagged_fills)}/{len(fills)} FOMO-reported fills tagged FOMO by the on-chain rule")
        for h in sorted(fills - set(tagged_fills)):
            print("  reported fill not tagged", h)
    connection.close()
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("command", choices=("measure", "evidence", "capture"))
    parser.add_argument("--rpc", default="http://127.0.0.1:8547")
    parser.add_argument("--market", default=DEFAULT_MARKET)
    parser.add_argument("--swaps", type=int, default=200)
    parser.add_argument("--head-offset", type=int, default=0,
                        help="extra blocks below head to sample from")
    parser.add_argument("--listener-window", type=int, default=900)
    parser.add_argument("--listener-budget", type=int, default=400000)
    parser.add_argument("--reported-minutes", type=int, default=60)
    parser.add_argument("--out", default=str(Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "flow_tags"))
    args = parser.parse_args(argv)
    return {"measure": measure, "evidence": evidence, "capture": capture}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
