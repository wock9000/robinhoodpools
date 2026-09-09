"""Current claims must unlock exact equity without another LP action."""
from copy import deepcopy

import pytest

from rhpools.lp_market_claims import fetch_position_claims
from rhpools.lp_market_protocols import (
    _selector, _v3_position_key, _v4_position_key, core_position_key,
)
from rhpools.lp_market_store import CanonicalConflict
from test_lp_market_service import (
    TOKEN, V3, header, ingest_effects, lp_effect, pools, position_state, service, swap,
)


def abi(*words):
    return "0x" + "".join((word % (1 << 256)).to_bytes(32, "big").hex() for word in words)


@pytest.mark.parametrize("protocol", ["v3", "v4"])
def test_lazy_fees_unlock_open_equity_and_net_pnl_at_a_canonical_pin(tmp_path, protocol):
    app = service(tmp_path / "claims.sqlite")
    try:
        app.store.upsert_pools(pools())
        opened_block, pin = header(100, 1_000), header(101, 1_001)
        opened = lp_effect(
            opened_block, protocol, "add", 1_000_000, (1_000, 1_000),
            position_state(0), position_state(1_000_000),
        )
        raw_key = (
            _v3_position_key(TOKEN, -10, 10) if protocol == "v3"
            else _v4_position_key(TOKEN, -10, 10, "0x" + "00" * 32)
        )
        opened["position_key"] = core_position_key(protocol, opened["pool_id"], raw_key)
        opened["data"]["core_position_key"] = raw_key
        opened.update(fee_amount0="0", fee_amount1="0")
        opened["data"].update(
            trace_complete=True, fees_accrued_exact=True, principal_delta_exact=True,
            principal_delta={"amount0": "-1000", "amount1": "-1000"},
        )
        app.store.ingest([opened_block, pin], [opened], cursor={
            "from_block": 100, "to_block": 101, "block_number": 101,
            "block_hash": pin["hash"], "timestamp": 1_001,
        }, transactions=[{
            "tx_hash": opened["tx_hash"], "block_number": 100,
            "block_hash": opened_block["hash"], "payer": TOKEN, "gas_usd": 0.01,
        }])
        position = dict(app.store.read().execute("SELECT * FROM lp_accounting_positions").fetchone())
        before = app.book.owner(TOKEN, {"window": "all"})["summary"]
        assert before["net_pnl_usd"] is None

        responses = {
            _selector("positions(bytes32)"): abi(1_000_000, 0, 0, 0, 0),
            _selector("slot0()"): abi(1 << 96, 0, 0, 0, 0, 0, 1),
            _selector("liquidity()"): abi(1_000_000),
            _selector("feeGrowthGlobal0X128()"): abi(1 << 128),
            _selector("feeGrowthGlobal1X128()"): abi(2 << 128),
            _selector("ticks(int24)"): abi(1_000_000, 0, 0, 0, 0, 0, 0, 1),
            _selector("getPositionInfo(bytes32,bytes32)"): abi(1_000_000, 0, 0),
            _selector("getSlot0(bytes32)"): abi(1 << 96, 0, 0, 3_000),
            _selector("getLiquidity(bytes32)"): abi(1_000_000),
            _selector("getFeeGrowthInside(bytes32,int24,int24)"): abi(1 << 128, 2 << 128),
        }

        class PinnedRpc:
            block_hash = pin["hash"]

            def batch(self, calls, **_kwargs):
                return [responses[params[0]["data"][:10]] for _method, params in calls]

            def call(self, _method, _params):
                return {**pin, "hash": self.block_hash}

        client = PinnedRpc()
        claims = fetch_position_claims(app.store, app.book, app.prices, client, [position])
        assert app.book.publish_claims(claims)
        detail = app.book.owner(TOKEN, {"window": "all"})
        assert detail["summary"]["net_pnl_usd"] == pytest.approx(2.988998)
        assert detail["positions"][0]["equity_usd"] == pytest.approx(3.000998)
        assert detail["positions"][0]["valuation_block"] == 101

        corrected = deepcopy(opened)
        corrected.update(amount0="2000", cashflow0="-2000")
        corrected["data"]["principal_delta"]["amount0"] = "-2000"
        app.store.enrich([corrected])
        assert app.book.owner(TOKEN, {"window": "all"})["summary"]["net_pnl_usd"] is None
        assert not app.book.publish_claims(claims)
        position = dict(app.store.read().execute("SELECT * FROM lp_accounting_positions").fetchone())
        refreshed = fetch_position_claims(app.store, app.book, app.prices, client, [position])
        assert app.book.publish_claims(refreshed)
        assert app.book.owner(TOKEN, {"window": "all"})["summary"]["net_pnl_usd"] == pytest.approx(2.987998)

        pin = header(102, 1_002)
        app.store.ingest([pin], [], cursor={
            "from_block": 102, "to_block": 102, "block_number": 102,
            "block_hash": pin["hash"], "timestamp": 1_002,
        })
        client.block_hash = pin["hash"]
        responses[_selector("liquidity()")] = abi(0)
        responses[_selector("getLiquidity(bytes32)")] = abi(0)
        for selector in ("slot0()", "getSlot0(bytes32)"):
            responses[_selector(selector)] = abi((1 << 160) - 1, 887271, 0, 0, 0, 0, 1)
        empty = fetch_position_claims(app.store, app.book, app.prices, client, [position])
        # A prior trusted mark is usable; the empty pool's arbitrary limit is not.
        assert empty[0]["price0_usd"] is None or empty[0]["price0_usd"] == pytest.approx(1.0)

        client.block_hash = "0x" + "ff" * 32
        with pytest.raises(CanonicalConflict):
            fetch_position_claims(app.store, app.book, app.prices, client, [position])
        app.store.rollback(100)
        assert app.book.owner(TOKEN, {"window": "all"})["summary"]["net_pnl_usd"] is None
    finally:
        app.close()


def test_every_wallet_on_a_full_owner_page_receives_recovery_work(tmp_path):
    app = service(tmp_path / "visible-wallets.sqlite")
    try:
        owners = [f"0x{number:040x}" for number in range(1, 201)]
        app.claims.request(owners)
        scheduled = [app.claims._next()[0] for _ in owners]
        assert set(scheduled) == set(owners)
    finally:
        app.close()


def test_rollback_does_not_publish_orphan_prices_for_surviving_claims(tmp_path):
    app = service(tmp_path / "claim-rollback.sqlite")
    try:
        app.store.upsert_pools(pools())
        blocks = [header(100 + index, 1_000 + index) for index in range(4)]
        opened = lp_effect(
            blocks[0], "v3", "add", 1_000, (1_000, 1_000),
            position_state(0), position_state(1_000),
        )
        removed = lp_effect(
            blocks[1], "v3", "remove", -1_000, (1_000, 1_000),
            position_state(1_000), position_state(0, 1_200, 1_000),
        )
        ingest_effects(app, blocks[:2], [opened, removed])
        canonical = swap(blocks[2], V3, "v3")
        canonical["sqrt_price_x96"] = str(2 << 96)
        app.store.ingest([blocks[2]], [canonical])
        position = dict(app.store.read().execute("SELECT * FROM lp_accounting_positions").fetchone())
        claim = {
            "position_key": position["position_key"], "epoch": 0,
            "block_number": 102, "block_hash": blocks[2]["hash"], "timestamp": 1_002,
            "position_last_block": position["last_block"],
            "position_last_tx_index": position["last_tx_index"],
            "position_last_log_index": position["last_log_index"],
            "position_pending_principal0": position["pending_principal0"],
            "position_pending_principal1": position["pending_principal1"],
            "position_pending_known": position["pending_known"],
            "position_state_json": position["state_json"],
            "liquidity": "0", "sqrt_price_x96": str(2 << 96), "tick": 13_863,
            "price0_usd": 4.0, "price1_usd": 1.0, "claim0": "1200", "claim1": "1000",
        }
        assert app.book.publish_claims([claim])
        params = {"window": "all", "identity_scope": "wallets"}
        verified = app.book.owner_candidates(params)["rows"][0]["gross_pnl_usd"]
        assert verified == pytest.approx(0.0038)
        orphan = swap(blocks[3], V3, "v3")
        orphan["sqrt_price_x96"] = str(3 << 96)
        app.store.ingest([blocks[3]], [orphan])
        app.store.rollback(102)
        assert app.book.owner_candidates(params)["rows"][0]["gross_pnl_usd"] is None
        claim["epoch"] = app.store.status()["epoch"]
        assert app.book.publish_claims([claim])
        assert app.book.owner_candidates(params)["rows"][0]["gross_pnl_usd"] == pytest.approx(verified)
    finally:
        app.close()
