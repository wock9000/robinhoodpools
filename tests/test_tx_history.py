from __future__ import annotations

import threading
from collections import OrderedDict

import pytest
from eth_abi import encode

from rhpools.tx_chain import PANCAKE_SMART_ROUTER, TOPIC_TRANSFER, TOPIC_V3_SWAP, UR, WETH
from rhpools.tx_core import HISTORY_BLOCKS_PER_DAY, HISTORY_DAYS, TOPIC_WITHDRAWAL, TxCore
from rhpools.tx_trade_store import TradeStore
from rhpools.tx_plan import TxError

WALLET = "0x" + "44" * 20
OTHER = "0x" + "55" * 20
TOKEN = "0x" + "66" * 20
TOKEN2 = "0x" + "77" * 20
TOPIC_WALLET = "0x" + WALLET[2:].rjust(64, "0")


def transfer(tx_hash, block, index, token, sender, recipient, value):
    return {"transactionHash": tx_hash, "blockNumber": hex(block), "transactionIndex": hex(index),
            "address": token, "topics": [TOPIC_TRANSFER, "0x" + sender[2:].rjust(64, "0"),
                                      "0x" + recipient[2:].rjust(64, "0")], "data": hex(value)}


class HistoryRPC:
    def __init__(self, entries, latest=HISTORY_BLOCKS_PER_DAY * HISTORY_DAYS + 10):
        self.entries = entries
        self.latest = latest
        self.calls = []
        self.traces = {}

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_getBlockByNumber":
            block = self.latest if params[0] == "latest" else int(params[0], 16)
            return {"number": hex(block), "hash": "0x" + "ab" * 32, "timestamp": hex(block * 2)}
        if method == "eth_getLogs":
            filt = params[0]
            lower, upper = int(filt["fromBlock"], 16), int(filt["toBlock"], 16)
            pos = 1 if filt["topics"][1] is not None else 2
            assert filt["topics"][0] == TOPIC_TRANSFER and filt["topics"][pos] == TOPIC_WALLET
            return [log for entry in self.entries.values() for log in entry["logs"]
                    if len(log["topics"]) == 3 and log["topics"][0] == TOPIC_TRANSFER
                    and lower <= int(log["blockNumber"], 16) <= upper and log["topics"][pos] == TOPIC_WALLET]
        if method == "eth_getTransactionByHash":
            return self.entries[params[0]]["tx"]
        if method == "eth_getTransactionReceipt":
            return {"logs": self.entries[params[0]]["logs"]}
        if method == "debug_traceTransaction":
            assert params[1] == {"tracer": "callTracer"}
            return self.traces.get(params[0], {"type": "CALL", "from": WALLET, "to": UR, "value": "0x0"})
        if method == "eth_call":
            selector = params[0]["data"][:10]
            if selector == "0x313ce567":
                return "0x" + f"{6 if params[0]['to'] == TOKEN else 8:064x}"
            return "0x" + encode(["string"], ["ABC" if params[0]["to"] == TOKEN else "XYZ"]).hex()
        raise AssertionError(method)


def make_core(rpc, clock):
    core = object.__new__(TxCore)
    core.rpc = rpc
    core._clock = lambda: clock[0]
    core.trade_store = None
    core._lock = threading.Lock()
    core._token_meta = {}
    core._history_txs = OrderedDict()
    core._history_wallets = OrderedDict()
    return core


def entry(index, block, logs, sender=WALLET, router=UR, value=0):
    tx_hash = "0x" + f"{index:064x}"
    for log in logs:
        log.update(transactionHash=tx_hash, blockNumber=hex(block), transactionIndex="0x0")
    return tx_hash, {"logs": logs, "tx": {"from": sender, "to": router, "value": hex(value)}}


def methods(rpc, method):
    return [params for name, params in rpc.calls if name == method]


def test_history_groups_flows_and_labels_newest_first_and_caches_wallet():
    block = HISTORY_BLOCKS_PER_DAY * HISTORY_DAYS + 10
    old_hash, old = entry(1, block - 2, [transfer("", block - 2, 0, TOKEN, WALLET, OTHER, 13)], value=8)
    new_hash, new = entry(2, block, [transfer("", block, 0, TOKEN, WALLET, OTHER, 3),
                                      transfer("", block, 0, TOKEN, WALLET, OTHER, 4),
                                      transfer("", block, 0, TOKEN2, OTHER, WALLET, 22)], router=PANCAKE_SMART_ROUTER)
    rpc = HistoryRPC({old_hash: old, new_hash: new}, block)
    clock = [100]
    core = make_core(rpc, clock)
    rows = core.history(WALLET)["rows"]
    assert [row["hash"] for row in rows] == [new_hash, old_hash]
    assert rows[0] == {"hash": new_hash, "block": block, "timestamp": block * 2,
                       "sent": [{"token": TOKEN, "amount": "7", "symbol": "ABC", "decimals": 6}],
                       "received": [{"token": TOKEN2, "amount": "22", "symbol": "XYZ", "decimals": 8}], "via": "Pancake", "source": "chain"}
    assert rows[1]["sent"] == [{"token": TOKEN, "amount": "13", "symbol": "ABC", "decimals": 6},
                                {"token": "native", "amount": "8", "symbol": "ETH", "decimals": 18}]
    assert rows[1]["via"] == "UR"
    assert len(methods(rpc, "eth_getLogs")) == 20
    assert core.history(WALLET)["rows"] == rows
    assert len(methods(rpc, "eth_getLogs")) == 20
    clock[0] += 16
    assert core.history(WALLET)["rows"] == rows
    assert len(methods(rpc, "eth_getTransactionReceipt")) == 2


def test_history_traces_inbound_native_for_withdrawal_and_empty_receives():
    block = HISTORY_BLOCKS_PER_DAY * HISTORY_DAYS + 10
    withdrawn_hash, withdrawn = entry(3, block, [transfer("", block, 0, TOKEN, WALLET, OTHER, 1),
        transfer("", block, 0, TOKEN2, OTHER, WALLET, 2),
        {"address": WETH, "topics": [TOPIC_WITHDRAWAL, "0x" + UR[2:].rjust(64, "0")], "data": "0x5"}])
    empty_hash, empty = entry(4, block - 1, [transfer("", block - 1, 0, TOKEN, WALLET, OTHER, 2),
        {"address": OTHER, "topics": [TOPIC_V3_SWAP], "data": "0x"}], sender=OTHER, router=OTHER)
    rpc = HistoryRPC({withdrawn_hash: withdrawn, empty_hash: empty}, block)
    rpc.traces[withdrawn_hash] = {"type": "CALL", "from": WALLET, "to": UR, "value": "0x0", "calls": [
        {"type": "CALL", "from": UR, "to": WALLET, "value": "0x3"},
        {"type": "CALL", "from": UR, "to": OTHER, "value": "0x7", "calls": [
            {"type": "CALL", "from": OTHER, "to": WALLET, "value": "0x2"}]}]}
    rpc.traces[empty_hash] = {"type": "CALL", "from": OTHER, "to": WALLET, "value": "0x9"}
    rows = make_core(rpc, [100]).history(WALLET)["rows"]
    assert rows[0]["received"][-1] == {"token": "native", "amount": "5", "symbol": "ETH", "decimals": 18}
    assert rows[1]["received"] == [{"token": "native", "amount": "9", "symbol": "ETH", "decimals": 18}]
    assert len(methods(rpc, "debug_traceTransaction")) == 2


def test_history_caps_rows_and_traces_without_extra_log_requests():
    block = HISTORY_BLOCKS_PER_DAY * HISTORY_DAYS + 10
    entries = dict(entry(i, block - 1, [transfer("", block - 1, 0, TOKEN, WALLET, OTHER, i)]) for i in range(1, 61))
    rpc = HistoryRPC(entries, block)
    rows = make_core(rpc, [100]).history(WALLET)["rows"]
    assert len(rows) == 20
    assert rows[0]["hash"] == "0x" + f"{60:064x}"
    assert rows[-1]["hash"] == "0x" + f"{41:064x}"
    assert len(methods(rpc, "debug_traceTransaction")) == 20
    assert len(methods(rpc, "eth_getLogs")) == 20
    logs = methods(rpc, "eth_getLogs")
    assert all(int(params[0]["toBlock"], 16) - int(params[0]["fromBlock"], 16) < HISTORY_BLOCKS_PER_DAY for params in logs)


def test_history_skips_wallet_transfers_and_lp_without_losing_older_trade():
    block = HISTORY_BLOCKS_PER_DAY * HISTORY_DAYS + 10
    unrelated = dict(entry(i, block, [transfer("", block, 0, TOKEN, WALLET, OTHER, i)],
                           router=OTHER) for i in range(2, 53))
    trade_hash, trade = entry(1, block - HISTORY_BLOCKS_PER_DAY - 1,
                              [transfer("", block - HISTORY_BLOCKS_PER_DAY - 1, 0,
                                        TOKEN, WALLET, OTHER, 10),
                               {"address": OTHER, "topics": [TOPIC_V3_SWAP], "data": "0x"}],
                              router=OTHER)
    rpc = HistoryRPC({**unrelated, trade_hash: trade}, block)
    assert [row["hash"] for row in make_core(rpc, [100]).history(WALLET)["rows"]] == [trade_hash]
    assert len(methods(rpc, "eth_getLogs")) == 20


def test_history_merges_recent_chain_with_durable_trades_and_pages_old_rows(tmp_path):
    latest = HISTORY_BLOCKS_PER_DAY * 20
    recent_hash, recent = entry(800, latest - 1,
                                [transfer("", latest - 1, 0, TOKEN, WALLET, OTHER, 4)])
    external_hash, external = entry(801, latest,
                                    [transfer("", latest, 0, TOKEN, OTHER, WALLET, 9)])
    rpc = HistoryRPC({recent_hash: recent, external_hash: external}, latest)
    store = TradeStore(tmp_path / "trades.sqlite")
    raw = {"kind": "swap", "sent": [{"token": TOKEN, "amount": "4"}], "received": [],
           "via": "UR", "fee": {"currency": TOKEN, "amount": "1"}}
    store.save(WALLET, {**raw, "hash": recent_hash, "block": latest - 1, "timestamp": 42})
    oldest = latest - HISTORY_DAYS * HISTORY_BLOCKS_PER_DAY - 100
    for index in range(55):
        store.save(WALLET, {**raw, "hash": "0x" + f"{index + 1:064x}",
                            "block": oldest - index, "timestamp": index})
    core = make_core(rpc, [100])
    core.trade_store = store
    first = core.history(WALLET)
    assert len(first["rows"]) == 50
    assert [row["hash"] for row in first["rows"][:2]] == [external_hash, recent_hash]
    assert first["rows"][0]["source"] == "chain"
    assert first["rows"][1]["source"] == "rhpools"
    assert first["rows"][1]["sent"] == [{"token": TOKEN, "amount": "4", "symbol": "ABC", "decimals": 6}]
    assert first["rows"][1]["fee"] == raw["fee"]
    assert first["next_before"] == oldest - 47
    assert len(methods(rpc, "eth_getTransactionReceipt")) == 1
    assert min(int(params[0]["fromBlock"], 16) for params in methods(rpc, "eth_getLogs")) == latest - HISTORY_DAYS * HISTORY_BLOCKS_PER_DAY + 1
    rpc.calls.clear()
    second = core.history(WALLET, first["next_before"])
    assert len(second["rows"]) == 7
    assert second["next_before"] is None
    assert second["rows"][-1]["block"] == oldest - 54
    assert not methods(rpc, "eth_getLogs")
    assert not methods(rpc, "eth_getTransactionByHash")


def test_history_refuses_bad_wallet_before_rpc():
    rpc = HistoryRPC({})
    with pytest.raises(TxError) as error:
        make_core(rpc, [100]).history("not-an-address")
    assert error.value.code == "invalid_intent"
    assert not rpc.calls
