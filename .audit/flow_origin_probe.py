"""Tally who submits swaps in Pons V2 pools and how FOMO wallets look on chain."""
import collections
import json
import sys
import urllib.request

RPC = "http://127.0.0.1:8547"
POOL_MANAGER = "0x8366a39cc670b4001a1121b8f6a443a643e40951"
PONS_HOOK = "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044"
SWAP_TOPIC = "0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"
BLOCKS = int(sys.argv[1]) if len(sys.argv) > 1 else 20_000


def rpc(batch):
    body = json.dumps([
        {"jsonrpc": "2.0", "id": i, "method": m, "params": p}
        for i, (m, p) in enumerate(batch)
    ]).encode()
    request = urllib.request.Request(RPC, body, {"Content-Type": "application/json"})
    replies = sorted(json.load(urllib.request.urlopen(request, timeout=60)), key=lambda r: r["id"])
    for reply in replies:
        if "error" in reply:
            raise RuntimeError(reply["error"])
    return [reply["result"] for reply in replies]


head = int(rpc([("eth_blockNumber", [])])[0], 16)
logs = []
for start in range(head - BLOCKS, head, 5_000):
    logs += rpc([("eth_getLogs", [{
        "address": POOL_MANAGER, "topics": [SWAP_TOPIC],
        "fromBlock": hex(start), "toBlock": hex(min(head, start + 4_999)),
    }])])[0]
pool_ids = sorted({log["topics"][1] for log in logs})
registered = {}
for offset in range(0, len(pool_ids), 200):
    chunk = pool_ids[offset:offset + 200]
    results = rpc([("eth_call", [{"to": PONS_HOOK, "data": "0xad091230" + pid[2:]}, "latest"]) for pid in chunk])
    for pid, result in zip(chunk, results):
        registered[pid] = len(result) > 2 and int(result[2:66] or "0", 16) != 0
pons_logs = [log for log in logs if registered.get(log["topics"][1])]
tx_hashes = sorted({log["transactionHash"] for log in pons_logs})
transactions = []
for offset in range(0, len(tx_hashes), 200):
    transactions += rpc([("eth_getTransactionByHash", [h]) for h in tx_hashes[offset:offset + 200]])
senders = sorted({tx["from"] for tx in transactions})
codes = {}
for offset in range(0, len(senders), 200):
    chunk = senders[offset:offset + 200]
    for sender, code in zip(chunk, rpc([("eth_getCode", [s, "latest"]) for s in chunk])):
        codes[sender] = code
to_counts = collections.Counter(tx["to"] for tx in transactions)
type_counts = collections.Counter(tx.get("type") for tx in transactions)
delegated = collections.Counter(
    ("0x" + codes[tx["from"]][8:48]) if codes[tx["from"]].startswith("0xef0100") else ("eoa" if codes[tx["from"]] == "0x" else "contract")
    for tx in transactions
)
print(json.dumps({
    "blocks": BLOCKS, "head": head, "v4_swaps": len(logs), "v4_pools": len(pool_ids),
    "pons_pools": sum(registered.values()), "pons_swaps": len(pons_logs),
    "pons_transactions": len(transactions),
    "tx_to_top": to_counts.most_common(8),
    "tx_type": type_counts.most_common(),
    "sender_code": delegated.most_common(8),
}, indent=1))
