from __future__ import annotations

import hashlib
import json

import pytest

from rhpools import workbench_actions as actions

POOL = "0x1111111111111111111111111111111111111111"
TOKEN0 = "0x2222222222222222222222222222222222222222"
TOKEN1 = "0x3333333333333333333333333333333333333333"
OWNER = "0x4444444444444444444444444444444444444444"
OTHER = "0x5555555555555555555555555555555555555555"


def _word(value: int) -> str:
    return f"{value & ((1 << 256) - 1):064x}"


def _encoded(*values: int) -> str:
    return "0x" + "".join(_word(value) for value in values)


def _address_word(value: str) -> str:
    return "0x" + value[2:].rjust(64, "0")


class FakeRPC:
    def __init__(self) -> None:
        self.chain_id = actions.CHAIN_ID
        self.block = 100
        self.block_hash = "0x" + "ab" * 32
        self.position_liquidity = 1_000_000_000
        self.owed0 = 1_000
        self.owed1 = 2_000
        self.fee_growth0_last = 0
        self.fee_growth1_last = 0
        self.fee_growth_global0 = 0
        self.fee_growth_global1 = 0
        self.pool_factory = actions.V3_FACTORY
        self.factory_pool = POOL
        self.sqrt_price = 1 << 96
        self.tick = 0
        self.calls: list[tuple[str, list]] = []

    def __call__(self, method, params, **_kwargs):
        self.calls.append((method, params))
        if method == "eth_chainId":
            return hex(self.chain_id)
        if method == "eth_getBlockByNumber":
            requested = params[0]
            number = self.block if requested == "latest" else int(requested, 16)
            return {"number": hex(number), "hash": self.block_hash}
        if method == "eth_getCode":
            address = params[0].lower()
            if address in {POOL, TOKEN0, TOKEN1, actions.V3_FACTORY}:
                return "0x6002"
            return "0x"
        if method == "eth_estimateGas":
            return hex(80_000)
        if method != "eth_call":
            raise AssertionError(f"unexpected RPC method {method}")

        request = params[0]
        target = request["to"].lower()
        data = request["data"].lower()
        selector = data[2:10]
        if target == POOL:
            if selector == actions.SEL_FACTORY:
                return _address_word(self.pool_factory)
            if selector == actions.SEL_TOKEN0:
                return _address_word(TOKEN0)
            if selector == actions.SEL_TOKEN1:
                return _address_word(TOKEN1)
            if selector == actions.SEL_FEE:
                return _encoded(500)
            if selector == actions.SEL_TICK_SPACING:
                return _encoded(60)
            if selector == actions.SEL_SLOT0:
                return _encoded(self.sqrt_price, self.tick, 0, 0, 0, 0, 1)
            if selector == actions.SEL_POSITIONS:
                return _encoded(
                    self.position_liquidity,
                    self.fee_growth0_last,
                    self.fee_growth1_last,
                    self.owed0,
                    self.owed1,
                )
            if selector == actions.SEL_FEE_GROWTH0:
                return _encoded(self.fee_growth_global0)
            if selector == actions.SEL_FEE_GROWTH1:
                return _encoded(self.fee_growth_global1)
            if selector == actions.SEL_TICKS:
                return _encoded(0, 0, 0, 0, 0, 0, 0, 1)
            if selector == actions.SEL_BURN:
                return _encoded(3_000, 4_000)
            if selector == actions.SEL_COLLECT:
                return _encoded(self.owed0, self.owed1)
        if target == actions.V3_FACTORY and selector == actions.SEL_GET_POOL:
            return _address_word(self.factory_pool)
        if target in (TOKEN0, TOKEN1):
            if selector == actions.SEL_DECIMALS:
                return _encoded(6)
        raise AssertionError(f"unexpected eth_call target={target} selector={selector}")


@pytest.fixture
def service():
    rpc = FakeRPC()
    rpc.call = rpc.__call__
    return actions.ActionService(rpc), rpc


def _payload(**updates):
    result = {
        "pool_id": POOL,
        "owner": OWNER,
        "action": "remove",
        "tick_lower": -60,
        "tick_upper": 60,
        "amount0": "0",
        "amount1": "0",
        "liquidity_bps": 10_000,
        "slippage_bps": 100,
    }
    result.update(updates)
    return result




def test_add_is_retired_without_reading_chain_or_caching_a_quote(service):
    svc, rpc = service

    with pytest.raises(actions.ActionError, match="adding liquidity is not supported.*unsafe"):
        svc.simulate(_payload(action="add", amount0="100", amount1="100"))

    assert rpc.calls == []
    assert svc._quotes == {}


@pytest.mark.parametrize(
    ("action", "step_id"),
    (("add", "mint"), ("add", "approve-token0"), ("remove", "approve-token1")),
)
def test_prepare_rejects_legacy_add_and_approval_quotes(
    service, monkeypatch, action, step_id
):
    svc, rpc = service
    monkeypatch.setattr(actions.time, "time", lambda: 1_000)
    simulation_id = "ab" * 32
    binding = {"owner": OWNER, "action": action, "expires_at": 1_001}
    canonical = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
    svc._quotes[simulation_id] = {
        "binding": binding,
        "binding_hash": hashlib.sha256(canonical).hexdigest(),
        "steps": {},
    }

    with pytest.raises(actions.ActionError, match="adding liquidity is not supported.*unsafe"):
        svc.prepare(
            {
                "simulation_id": simulation_id,
                "owner": OWNER,
                "step_id": step_id,
            }
        )

    assert rpc.calls == []

def test_wrong_chain_and_owner_are_rejected(service):
    svc, rpc = service
    rpc.chain_id = 1
    with pytest.raises(actions.ActionError, match="wrong chain"):
        svc.simulate(_payload())

    rpc.chain_id = actions.CHAIN_ID
    quote = svc.simulate(_payload())
    with pytest.raises(actions.ActionError, match="does not match"):
        svc.prepare(
            {
                "simulation_id": quote["simulation_id"],
                "owner": OTHER,
                "step_id": "burn",
            }
        )

    rpc.chain_id = 1
    with pytest.raises(actions.ActionError, match="wrong chain"):
        svc.prepare(
            {
                "simulation_id": quote["simulation_id"],
                "owner": OWNER,
                "step_id": "burn",
            }
        )


def test_expired_quote_requires_rebuild(service, monkeypatch):
    svc, _rpc = service
    now = {"value": 1_000}
    monkeypatch.setattr(actions.time, "time", lambda: now["value"])
    quote = svc.simulate(_payload())
    now["value"] += actions.QUOTE_TTL_S

    with pytest.raises(actions.ActionError, match="expired.*rebuild"):
        svc.prepare(
            {
                "simulation_id": quote["simulation_id"],
                "owner": OWNER,
                "step_id": "burn",
            }
        )


def test_bad_tick_spacing_and_range_are_rejected(service):
    svc, _rpc = service
    with pytest.raises(actions.ActionError, match="tick spacing 60"):
        svc.simulate(_payload(tick_lower=-59))
    with pytest.raises(actions.ActionError, match="lower < upper"):
        svc.simulate(_payload(tick_lower=60, tick_upper=60))


def test_factory_membership_is_required_for_direct_pool_actions(service):
    svc, rpc = service
    rpc.pool_factory = OTHER
    with pytest.raises(actions.ActionError, match="allowlisted canonical V3 factory"):
        svc.simulate(_payload())

    rpc.pool_factory = actions.V3_FACTORY
    rpc.factory_pool = OTHER
    with pytest.raises(actions.ActionError, match="factory getPool"):
        svc.simulate(_payload())




def test_prepare_refuses_price_move_beyond_slippage_ceiling(service):
    svc, rpc = service
    quote = svc.simulate(_payload())
    rpc.sqrt_price = actions.sqrt_ratio_at_tick(200)
    rpc.tick = 200

    with pytest.raises(actions.ActionError, match="price moved beyond slippage_bps"):
        svc.prepare(
            {
                "simulation_id": quote["simulation_id"],
                "owner": OWNER,
                "step_id": "burn",
            }
        )


def test_remove_is_direct_burn_then_collect_and_rechecks_owned_liquidity(service):
    svc, rpc = service
    quote = svc.simulate(
        _payload(
            action="remove",
            amount0="0",
            amount1="0",
            liquidity_bps=5_000,
        )
    )

    assert quote["ready"] is True
    assert [(step["id"], step["kind"]) for step in quote["steps"]] == [
        ("burn", "remove"),
        ("collect", "collect"),
    ]
    assert all(step["transaction"]["to"] == POOL for step in quote["steps"])
    assert quote["steps"][0]["transaction"]["data"][2:10] == actions.SEL_BURN
    assert quote["steps"][1]["transaction"]["data"][2:10] == actions.SEL_COLLECT
    assert actions._selector("burnAndCollect(address,int24,int24,uint128)") not in {
        step["transaction"]["data"][2:10] for step in quote["steps"]
    }
    assert OWNER[2:].rjust(64, "0") == quote["steps"][1]["transaction"]["data"][10:74]

    prepared_burn = svc.prepare(
        {
            "simulation_id": quote["simulation_id"],
            "owner": OWNER,
            "step_id": "burn",
        }
    )
    assert prepared_burn["transaction"] == quote["steps"][0]["transaction"]

    rpc.position_liquidity = int(quote["summary"]["liquidity"]) - 1
    with pytest.raises(actions.ActionError, match="below the quoted burn amount"):
        svc.prepare(
            {
                "simulation_id": quote["simulation_id"],
                "owner": OWNER,
                "step_id": "burn",
            }
        )


def test_collect_pokes_lazy_fees_then_collects_without_resimulation(service):
    svc, rpc = service
    rpc.owed0 = 0
    rpc.owed1 = 0
    rpc.fee_growth_global0 = 1 << 128
    quote = svc.simulate(
        _payload(
            action="collect",
            amount0="0",
            amount1="0",
            liquidity_bps=0,
        )
    )

    assert quote["ready"] is True
    assert [(step["id"], step["kind"]) for step in quote["steps"]] == [
        ("poke", "poke"),
        ("collect", "collect"),
    ]
    poke = quote["steps"][0]["transaction"]
    collect = quote["steps"][1]["transaction"]
    assert poke["from"] == collect["from"] == OWNER
    assert poke["to"] == collect["to"] == POOL
    assert poke["data"][2:10] == actions.SEL_BURN
    assert int(poke["data"][-64:], 16) == 0
    assert collect["data"][2:10] == actions.SEL_COLLECT
    assert quote["summary"]["amount0"] == "1000"
    assert quote["summary"]["amount1"] == "0"

    prepared_poke = svc.prepare(
        {
            "simulation_id": quote["simulation_id"],
            "owner": OWNER,
            "step_id": "poke",
        }
    )
    prepared_collect = svc.prepare(
        {
            "simulation_id": quote["simulation_id"],
            "owner": OWNER,
            "step_id": "collect",
        }
    )
    assert prepared_poke["transaction"] == poke
    assert prepared_collect["transaction"] == collect

    with pytest.raises(actions.ActionError, match="unexpected prepare fields: data"):
        svc.prepare(
            {
                "simulation_id": quote["simulation_id"],
                "owner": OWNER,
                "step_id": "collect",
                "data": "0xdeadbeef",
            }
        )


def test_zero_liquidity_credited_position_collects_without_poke(service):
    svc, rpc = service
    rpc.position_liquidity = 0
    quote = svc.simulate(
        _payload(
            action="collect",
            amount0="0",
            amount1="0",
            liquidity_bps=0,
        )
    )

    assert quote["ready"] is True
    assert [step["id"] for step in quote["steps"]] == ["collect"]
    assert quote["steps"][0]["transaction"]["data"][2:10] == actions.SEL_COLLECT
