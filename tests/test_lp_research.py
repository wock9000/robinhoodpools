"""Financial coverage, identity and raw-unit boundaries for LP research."""

import time

import pytest

from rhpools.lp_research import LPResearchService

OWNER = "0x" + "11" * 20
MANAGER = "0x" + "22" * 20
POOL_V3 = "0x" + "33" * 20
POOL_V4 = "0x" + "44" * 32
TOKEN0 = "0x" + "55" * 20
TOKEN1 = "0x" + "66" * 20


class _Cursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, pools):
        self.pools = pools

    def execute(self, _sql, pool_ids):
        return _Cursor([self.pools[pool_id] for pool_id in pool_ids if pool_id in self.pools])


class _Store:
    def __init__(self, pools):
        self.connection = _Connection(pools)

    def read(self):
        return self.connection


class _LPService:
    def __init__(self, detail, pools, activity=None):
        self.detail = detail
        self.store = _Store(pools)
        self.activity = activity
        self.owner_calls = 0

    def owner(self, params):
        self.owner_calls += 1
        assert params["owner"] == OWNER
        return self.detail

    def _current_owner_snapshot(self, params):
        assert params["q"] == OWNER
        rows = () if self.activity is None else ({
            "owner": OWNER, "custody": None, "activity": self.activity,
        },)
        return {
            "rows": rows,
            "status": {
                "qualification": "provisional_canonical", "head": 1_020,
                "observed_from": 1_010,
            },
        }


def _pool(pool_id, protocol, fee, *, current_fee=None):
    return {
        "id": pool_id, "protocol": protocol,
        "token0": TOKEN0, "token1": TOKEN1,
        "symbol0": "ASSET", "symbol1": "USDG",
        "decimals0": 18, "decimals1": 6,
        "fee_ppm": fee, "current_fee_ppm": current_fee,
        "tick_spacing": 10, "hook": None, "current_tick": 0,
        "valuation_block": 1_000, "valuation_timestamp": 2_000,
    }


def _position(key, *, owner=OWNER, custody=MANAGER, pool_id=POOL_V3,
              protocol="v3", principal=10.0, claim=1.0, principal0="10",
              principal1="20", pending0="1", pending1="2"):
    return {
        "position_key": key, "token_id": "0x10", "owner": owner,
        "custody": custody, "identity_basis": "verified_owner",
        "identity_match": "beneficial_owner" if owner == OWNER else "custody",
        "pool_id": pool_id, "pair": "ASSET/USDG", "protocol": protocol,
        "tick_lower": -100, "tick_upper": 100,
        "liquidity": "123456789012345678901234567890",
        "principal0": principal0, "principal1": principal1,
        "pending_principal0": pending0, "pending_principal1": pending1,
        "principal_usd": principal, "claim_principal_usd": claim,
        "valuation_block": 1_000, "valuation_timestamp": 2_000,
        "valuation_basis": "pool_current_v3_integer_principal",
        "status": "open", "last_event_at": 2_000,
        "coverage": {"history": "full", "identity": "verified", "qualified": True, "reasons": []},
        "episodes": [{
            "opened_at": 1_900, "closed_at": None, "duration_s": None,
            "status": "open",
        }],
    }


def _detail(positions, *, identity_basis="owner_and_custody"):
    return {
        "owner": OWNER, "custody": OWNER if identity_basis == "owner_and_custody" else None,
        "identity_basis": identity_basis, "positions": positions,
        # These values must never be republished as comparable strategy rankings.
        "summary": {"gross_pnl_usd": 9_999_999, "net_pnl_usd": 9_999_998},
        "coverage": {
            "state": "catching_up", "indexed_head": 1_000,
            "history_from": int(time.time()) - 40 * 86_400,
            "history_from_block": 100, "history_to": int(time.time()) - 20,
            "history_target": int(time.time()),
        },
    }


def test_known_usdg_subtotals_never_value_custody_or_unknown_components():
    complete = _position(
        "complete", principal0="1000000000000000000000000000000",
        principal1="2000000000000000000000000000000",
    )
    partial = _position(
        "partial", principal=5.0, claim=None,
        principal0="3000000000000000000000000000000", principal1="400",
        pending0=None, pending1=None,
    )
    custody = _position(
        "custody", owner=None, custody=OWNER, pool_id=POOL_V4, protocol="v4",
        principal=999.0, claim=1.0,
    )
    activity = {
        "block_number": 1_020, "timestamp": 2_020, "pool_id": POOL_V3,
        "pair": "ASSET/USDG", "kind": "add", "tx_hash": "0x" + "77" * 32,
        "event_count": 1, "qualification": "provisional_canonical",
        "identity_match": "beneficial_owner",
    }
    service = _LPService(
        _detail([complete, partial, custody]),
        {
            POOL_V3: _pool(POOL_V3, "v3", 3_000),
            POOL_V4: _pool(POOL_V4, "v4", 0x800000, current_fee=2_500),
        },
        activity,
    )

    result = LPResearchService(service).owner({"owner": OWNER, "window": "30d"})

    assert result["allocation"]["known_principal_usdg"] == pytest.approx(16.0)
    assert result["allocation"]["current_beneficial_positions"] == 2
    assert result["allocation"]["custody_positions_observed"] == 1
    assert result["allocation"]["valued_positions"] == 1
    assert result["allocation"]["partially_valued_positions"] == 1
    assert result["allocation"]["unvalued_positions"] == 0
    assert len(result["allocation"]["positions"]) == 2
    assert result["allocation"]["positions"][0]["liquidity_raw"] == complete["liquidity"]
    partial_result = next(
        row for row in result["allocation"]["positions"]
        if row["position_key"] == "partial"
    )
    assert partial_result["token_amounts"][0]["pending_claim_raw"] is None
    token0 = result["allocation"]["pools"][0]["token_amounts"][0]
    assert token0["known_principal_raw"] == "4000000000000000000000000000000"
    assert token0["principal_complete"] is True
    assert token0["pending_claim_complete"] is False
    assert "gross_pnl_usd" not in result and "net_pnl_usd" not in result
    assert result["activity"]["latest"] == activity
    assert result["coverage"]["identity"]["current_activity_match"] == "beneficial_owner"


def test_configuration_preserves_raw_fee_modes_and_never_sums_pool_liquidity():
    static = _position("static")
    dynamic = _position(
        "dynamic", owner=None, custody=OWNER, pool_id=POOL_V4, protocol="v4",
    )
    service = _LPService(
        _detail([static, dynamic]),
        {
            POOL_V3: _pool(POOL_V3, "v3", 3_000),
            POOL_V4: _pool(POOL_V4, "v4", 0x800000, current_fee=500),
        },
    )

    result = LPResearchService(service).owner({"owner": OWNER})
    by_key = {row["position_key"]: row for row in result["configurations"]["positions"]}

    assert by_key["static"]["fee"] == {
        "mode": "static", "raw_configuration": "3000",
        "configured_ppm": "3000", "current_ppm": "3000", "hook": None,
    }
    assert by_key["dynamic"]["fee"] == {
        "mode": "dynamic", "raw_configuration": str(0x800000),
        "configured_ppm": None, "current_ppm": "500", "hook": None,
    }
    assert by_key["static"]["token_id"] == "16"
    assert set(result["configurations"]["fee_mode_mix"][0]) == {
        "mode", "returned_positions", "pool_count",
    }
    assert "liquidity_raw" not in result["allocation"]["pools"][0]


def test_invalid_owner_and_position_boundary_are_explicit():
    empty_service = _LPService(_detail([]), {POOL_V3: _pool(POOL_V3, "v3", 3_000)})
    research = LPResearchService(empty_service)
    with pytest.raises(ValueError, match="20-byte hexadecimal"):
        research.owner({"owner": "0x1234"})
    assert empty_service.owner_calls == 0

    positions = [_position(f"position-{index}") for index in range(201)]
    service = _LPService(_detail(positions), {POOL_V3: _pool(POOL_V3, "v3", 3_000)})
    result = LPResearchService(service).owner({"owner": OWNER, "window": "all"})

    assert result["coverage"]["returned_positions"] == 200
    assert result["coverage"]["source_positions_received"] == 201
    assert result["coverage"]["positions_omitted_by_research_limit"] == 1
    assert result["coverage"]["source_position_limit"] == 200
    assert result["coverage"]["possible_position_truncation"] is True
    assert result["allocation"]["returned_position_scope"] is True
    assert result["configurations"]["returned_position_scope"] is True
    assert any("base 200-position page boundary" in item for item in result["limitations"])
