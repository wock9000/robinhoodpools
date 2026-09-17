from __future__ import annotations

import pytest
import time

from rhpools.lp_market_service import LPMarketService
from rhpools.workbench_market import USDG


def swap(block, pool_id, protocol):
    return {
        "block_number": int(block["number"], 16), "block_hash": block["hash"],
        "tx_hash": "0x" + f"{int(block['hash'], 16) * 100:064x}",
        "tx_index": 0, "log_index": 0, "timestamp": int(block["timestamp"], 16),
        "pool_id": pool_id, "protocol": protocol, "kind": "swap", "owner": None,
        "custody": None, "position_key": None, "token_id": None,
        "tick_lower": None, "tick_upper": None, "liquidity_delta": None,
        "liquidity": "1000000000000", "sqrt_price_x96": str(1 << 96), "tick": 0,
        "fee_ppm": 3_000,
        "amount0": "-100000000", "amount1": "101000000",
        "fee_amount0": None, "fee_amount1": None, "cashflow0": None, "cashflow1": None,
        "accounting_basis": "swap event", "identity_basis": "not a position", "data": {},
    }


def service(path):
    return LPMarketService(None, "http://127.0.0.1:1", path, start=False)


TOKEN_ADDR = "0x" + "12" * 20
V3 = "0x" + "23" * 20


def header(number, timestamp):
    return {"number": hex(number), "hash": "0x" + f"{number:064x}",
            "parentHash": "0x" + f"{number - 1:064x}", "timestamp": hex(timestamp)}


def test_history_ingest_without_project_queues_reprojection_and_prices_later(tmp_path):
    app = service(tmp_path / "market.sqlite")
    try:
        app.store.upsert_pools([
            {"id": V3, "address": V3, "protocol": "v3",
             "token0": TOKEN_ADDR, "token1": USDG,
             "symbol0": "ASSET", "symbol1": "USDG",
             "decimals0": 6, "decimals1": 6, "tick_spacing": 1, "hook": None,
             "factory": "0x1f7d7550b1b028f7571e69a784071f0205fd2efa",
             "created_block": 1, "source": "factory-live"},
        ])
        block = header(100, int(time.time()) - 60)
        event = swap(block, V3, "v3")
        inserted = app.store.ingest([block], [event], lane="history", project=False)
        assert len(inserted) == 1
        assert app.store.read().execute(
            "SELECT COUNT(*) FROM lp_price_samples", (),
        ).fetchone()[0] == 0
        pending = app.store.pending_reprojections(10)
        assert [row["id"] for row in pending] == [inserted[0]["id"]]
        app.store.reproject([inserted[0]["id"]])
        assert app.store.read().execute(
            "SELECT COUNT(*) FROM lp_price_samples", (),
        ).fetchone()[0] == 1
        assert app.store.pending_reprojections(10) == []
    finally:
        app.close()