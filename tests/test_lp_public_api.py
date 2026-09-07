"""Consumer-visible canonical and aggregation boundaries for the public LP API."""
from __future__ import annotations

from threading import RLock
from types import SimpleNamespace

import pytest
from eth_abi import decode as abi_decode
from eth_abi import encode as abi_encode
from eth_utils import keccak

from rhpools import _mc
from rhpools.lp_chain import CHAIN_ID
from rhpools.lp_market_protocols import (
    POOL_MANAGER,
    STATE_VIEW,
    V2_FACTORIES,
    V3_FACTORIES,
)
from rhpools.lp_market_store import MarketStore
from rhpools.lp_public_api import PublicAPIReorg, PublicMarketAPI
from rhpools.workbench_market import (
    LIQUIDITY_SELECTOR,
    NATIVE,
    RESERVES_SELECTOR,
    SV_LIQUIDITY_SELECTOR,
    SV_SLOT0_SELECTOR,
)

TOKEN = "0x" + "22" * 20
LOW = "0x" + "11" * 20
HIGH = "0x" + "33" * 20
V2_FACTORY = sorted(V2_FACTORIES)[0]
V3_FACTORY = sorted(V3_FACTORIES)[0]


def _v4_id(token0: str, token1: str, fee: int, spacing: int, hooks: str) -> str:
    encoded = b"".join((
        int(token0, 16).to_bytes(32, "big"),
        int(token1, 16).to_bytes(32, "big"),
        fee.to_bytes(32, "big"),
        spacing.to_bytes(32, "big", signed=True),
        int(hooks, 16).to_bytes(32, "big"),
    ))
    return "0x" + keccak(encoded).hex()


def _pool(
    pool_id: str,
    protocol: str,
    token0: str,
    token1: str,
    *,
    fee: int | None = None,
    spacing: int | None = None,
    hooks: str | None = None,
    configured_fee: int | None = None,
) -> dict:
    factory = POOL_MANAGER if protocol == "v4" else (
        V2_FACTORY if protocol == "v2" else V3_FACTORY
    )
    metadata = (
        {"configured_fee": configured_fee, "dynamic_fee": bool(configured_fee and configured_fee & 0x800000)}
        if protocol == "v4"
        else {"discovery_basis": "factory_creation_event"}
    )
    return {
        "id": pool_id,
        "protocol": protocol,
        "address": POOL_MANAGER if protocol == "v4" else pool_id,
        "token0": token0,
        "token1": token1,
        "symbol0": "TOKEN" if token0 == TOKEN else "OTHER",
        "symbol1": "TOKEN" if token1 == TOKEN else "OTHER",
        "decimals0": 6,
        "decimals1": 6,
        "fee_ppm": fee,
        "tick_spacing": spacing,
        "hook": hooks,
        "factory": factory,
        "created_block": 7,
        "source": "PoolManager.Initialize" if protocol == "v4" else "factory event",
        "metadata_json": metadata,
    }


def _insert(store: MarketStore, rows: list[dict]) -> None:
    with store.transaction() as connection:
        store._upsert_pools(connection, rows)


class SnapshotRpc:
    def __init__(self, responses: dict[tuple[str, str], bytes], *, reorg: bool = False):
        self.responses = responses
        self.reorg = reorg
        self.number = 123_456
        self.block_hash = "0x" + "ab" * 32
        self.multicalls = 0
        self.direct_state_batches = 0
        self.multicall_http_batches = 0
        self.state_block_tags: set[str] = set()

    @property
    def header(self) -> dict:
        return {
            "number": hex(self.number),
            "hash": self.block_hash,
            "parentHash": "0x" + "cd" * 32,
            "timestamp": hex(1_800_000_000),
        }

    def _state(self, target: str, data: str) -> bytes | None:
        return self.responses.get((target.lower(), data.lower()))

    def _multicall(self, params) -> str:
        request, block = params
        assert request["to"].lower() == _mc.MULTICALL3.lower()
        self.state_block_tags.add(block)
        packed = abi_decode(
            ["(address,bool,bytes)[]"], bytes.fromhex(request["data"][10:]),
        )[0]
        results = []
        for target, _allow_failure, calldata in packed:
            value = self._state(target, "0x" + bytes(calldata).hex())
            results.append((value is not None, value or b""))
        return "0x" + abi_encode(["(bool,bytes)[]"], [results]).hex()

    def batch(self, calls):
        calls = list(calls)
        if [method for method, _params in calls] == ["eth_chainId", "eth_getBlockByNumber"]:
            return [hex(CHAIN_ID), self.header]
        if all(
            method == "eth_call"
            and params[0]["to"].lower() == _mc.MULTICALL3.lower()
            for method, params in calls
        ):
            self.multicalls += len(calls)
            self.multicall_http_batches += 1
            return [self._multicall(params) for _method, params in calls]
        self.direct_state_batches += 1
        values = []
        for method, params in calls:
            assert method == "eth_call"
            value = self._state(params[0]["to"], params[0]["data"])
            if value is None:
                raise RuntimeError("sub-call unavailable")
            values.append("0x" + value.hex())
        return values

    def call(self, method, params):
        if method == "eth_getBlockByNumber":
            if self.reorg:
                return {**self.header, "hash": "0x" + "ef" * 32}
            return self.header
        assert method == "eth_call"
        self.multicalls += 1
        self.multicall_http_batches += 1
        return self._multicall(params)


class Service:
    def __init__(self, store: MarketStore, rpc: SnapshotRpc, catalog_pools=()):
        tokens = {
            TOKEN: SimpleNamespace(symbol="TOKEN", decimals=6),
            LOW: SimpleNamespace(symbol="LOW", decimals=6),
            HIGH: SimpleNamespace(symbol="HIGH", decimals=6),
        }
        catalog_pools = tuple(catalog_pools)
        universe = SimpleNamespace(
            pools=catalog_pools,
            by_id={pool.id: pool for pool in catalog_pools},
            tokens=tokens,
        )
        self.store = store
        self.market = SimpleNamespace(
            rpc=rpc,
            universe=universe,
            _discovered={},
            _lock=RLock(),
            catalog=lambda _params: {
                "counts": {
                    "v2": sum(pool.kind == "v2" for pool in catalog_pools),
                    "v3": sum(pool.kind == "v3" for pool in catalog_pools),
                    "v4": sum(pool.kind == "v4" for pool in catalog_pools),
                },
                "coverage": {
                    "universe": "known-factory-complete",
                    "historical_continuity": "complete",
                    "historical_gap": [],
                    "factory_tail": {
                        "state": "live",
                        "through_block": rpc.number,
                        "through_hash": rpc.block_hash,
                        "head_lag_blocks": 0,
                    },
                },
            },
        )

    def status(self):
        return {
            "state": "live",
            "indexed_head": self.market.rpc.number - 1,
            "lag_blocks": 1,
            "backfill": False,
            "coverage": {},
        }


def test_lookup_matches_both_currency_sides_and_recovers_canonical_dynamic_v4_key():
    v2 = "0x" + "a1" * 20
    v3 = "0x" + "b2" * 20
    configured_fee = 0x800000
    spacing = 60
    v4 = _v4_id(LOW, TOKEN, configured_fee, spacing, NATIVE)
    store = MarketStore(":memory:")
    _insert(store, [
        _pool(v2, "v2", LOW, TOKEN),
        _pool(
            v4, "v4", LOW, TOKEN, spacing=None, hooks=NATIVE,
            configured_fee=configured_fee,
        ),
    ])
    catalog_v3 = SimpleNamespace(
        id=v3, address=v3, kind="v3", token0=TOKEN, token1=HIGH,
        fee_ppm=500, tick_spacing=10, hook=None, dynamic_fee=False,
        factory=V3_FACTORY, created_block=8, source="census",
    )
    responses = {
        (v2, RESERVES_SELECTOR): abi_encode(
            ["uint112", "uint112", "uint32"], [2_000_000, 9_000_000, 10],
        ),
        (v3, LIQUIDITY_SELECTOR): abi_encode(["uint128"], [2**96 + 7]),
        (STATE_VIEW, SV_LIQUIDITY_SELECTOR + v4[2:]): abi_encode(["uint128"], [2**100 + 9]),
        (STATE_VIEW, SV_SLOT0_SELECTOR + v4[2:]): abi_encode(
            ["uint160", "int24", "uint24", "uint24"], [2**96, 0, 0, 2_750],
        ),
    }
    rpc = SnapshotRpc(responses)
    service = Service(store, rpc, [catalog_v3])
    api = PublicMarketAPI(service, cache_ttl=0)
    try:
        result = api.pools({"token": TOKEN.upper().replace("0X", "0x")})
        assert result["pool_count"] == 3
        assert {row["matched_currency"] for row in result["pools"]} == {
            "currency0", "currency1",
        }
        dynamic = next(row for row in result["pools"] if row["protocol"] == "v4")
        assert dynamic["pool_id"] == v4
        assert dynamic["pool_address"] is None
        assert dynamic["manager_address"] == POOL_MANAGER
        assert dynamic["pool_key"] == {
            "currency0": LOW,
            "currency1": TOKEN,
            "fee_raw": str(configured_fee),
            "dynamic_fee": True,
            "tick_spacing": str(spacing),
            "hooks": NATIVE,
        }
        assert dynamic["fee"] == {
            "mode": "dynamic",
            "configured_raw": str(configured_fee),
            "configured_ppm": None,
            "current_ppm": "2750",
            "current_status": "block_pinned",
        }
        assert dynamic["liquidity"]["active_liquidity_raw"] == str(2**100 + 9)
        assert result["snapshot"]["block_number"] == str(rpc.number)
        assert result["snapshot"]["block_hash"] == rpc.block_hash
        assert result["snapshot"]["canonical"] is True
        assert rpc.multicalls == 1
        assert rpc.direct_state_batches == 0
    finally:
        api.close()
        store.close()


def test_asset_groups_sum_only_comparable_token_side_reserves_and_keep_missing_unknown():
    v2_token0 = "0x" + "a1" * 20
    v2_token1 = "0x" + "a2" * 20
    v3 = "0x" + "b1" * 20
    store = MarketStore(":memory:")
    _insert(store, [
        _pool(v2_token0, "v2", TOKEN, HIGH),
        _pool(v2_token1, "v2", LOW, TOKEN),
        _pool(v3, "v3", TOKEN, HIGH, fee=3_000, spacing=60),
    ])
    responses = {
        (v2_token0, RESERVES_SELECTOR): abi_encode(
            ["uint112", "uint112", "uint32"], [12_500_000, 4_000_000, 10],
        ),
        # v2_token1 is intentionally absent: Multicall reports that sub-call false.
        (v3, LIQUIDITY_SELECTOR): abi_encode(["uint128"], [999_999_999]),
    }
    rpc = SnapshotRpc(responses)
    service = Service(store, rpc)
    api = PublicMarketAPI(service, cache_ttl=0)
    try:
        result = api.assets({"token": TOKEN})
        groups = {row["configuration"]["protocol"]: row for row in result["groups"]}
        v2_group = groups["v2"]
        assert v2_group["state_coverage"] == {
            "state": "partial", "measured_pools": 1, "missing_pools": 1,
        }
        assert v2_group["subtotals"] == [{
            "metric": "matched_currency_reserve",
            "currency": TOKEN,
            "value_raw": "12500000",
            "value_decimal": "12.5",
            "decimals": 6,
            "unit": "queried token units",
            "pools_measured": 1,
            "pools_missing": 1,
            "coverage": "partial",
            "basis": "sum of the queried currency side in block-pinned V2 reserves",
        }]
        assert groups["v3"]["subtotals"] == []
        assert "raw_active_liquidity_is_pool_specific_and_not_additive" in groups["v3"]["limitations"]
        assert result["coverage"]["aggregation"]["missing_state_pools"] == 1
        pool_rows = {row["pool_id"]: row for row in api.pools({"token": TOKEN})["pools"]}
        assert pool_rows[v2_token1]["liquidity"]["reserve0_raw"] is None
        assert pool_rows[v2_token1]["liquidity"]["reserve1_raw"] is None
        assert pool_rows[v2_token1]["liquidity"]["unavailable_reason"] == (
            "contract_call_reverted_or_unavailable"
        )
    finally:
        api.close()
        store.close()


def test_large_token_snapshot_batches_multicalls_without_hiding_pool_failures():
    pool_count = _mc.MAX_PER_BATCH + 1
    pool_ids = [f"0x{index + 1:040x}" for index in range(pool_count)]
    store = MarketStore(":memory:")
    _insert(store, [
        _pool(pool_id, "v3", TOKEN, HIGH, fee=500, spacing=10)
        for pool_id in pool_ids
    ])
    responses = {
        (pool_id, LIQUIDITY_SELECTOR): abi_encode(["uint128"], [index + 1])
        for index, pool_id in enumerate(pool_ids[:-1])
    }
    rpc = SnapshotRpc(responses)
    api = PublicMarketAPI(Service(store, rpc), cache_ttl=0)
    try:
        result = api.pools({"token": TOKEN})
        assert result["pool_count"] == pool_count
        assert result["coverage"]["state"] == {
            "requested_pools": pool_count,
            "available_pools": pool_count - 1,
            "unavailable_pools": 1,
        }
        missing = next(row for row in result["pools"] if row["pool_id"] == pool_ids[-1])
        assert missing["liquidity"]["unavailable_reason"] == (
            "contract_call_reverted_or_unavailable"
        )
        assert rpc.multicalls == 2
        assert rpc.multicall_http_batches == 1
        assert rpc.direct_state_batches == 0
        assert rpc.state_block_tags == {hex(rpc.number)}
    finally:
        api.close()
        store.close()


def test_native_catalog_recovers_shared_legacy_v4_configurations_completely():
    fee = 933_267
    hook = NATIVE
    spacings = (19_988, 9_303)
    pools = []
    expected: dict[str, int] = {}
    next_currency = 1
    for spacing in spacings:
        for complete in (True, *([False] * 32)):
            currency1 = f"0x{0x8000000000000000000000000000000000000000 + next_currency:040x}"
            next_currency += 1
            pool_id = _v4_id(NATIVE, currency1, fee, spacing, hook)
            pools.append(_pool(
                pool_id,
                "v4",
                NATIVE,
                currency1,
                fee=fee,
                spacing=spacing if complete else None,
                hooks=hook,
                configured_fee=fee,
            ))
            expected[pool_id] = spacing

    invalid_fee = 1_000_001
    invalid_currency = "0xffffffffffffffffffffffffffffffffffffffff"
    pools.append(_pool(
        _v4_id(NATIVE, invalid_currency, invalid_fee, 32_767, hook),
        "v4",
        NATIVE,
        invalid_currency,
        fee=invalid_fee,
        spacing=None,
        hooks=hook,
        configured_fee=invalid_fee,
    ))
    store = MarketStore(":memory:")
    _insert(store, pools)
    responses = {
        (STATE_VIEW, SV_LIQUIDITY_SELECTOR + pool_id[2:]): abi_encode(
            ["uint128"], [index + 1],
        )
        for index, pool_id in enumerate(expected)
    }
    api = PublicMarketAPI(Service(store, SnapshotRpc(responses)), cache_ttl=0)
    try:
        result = api.pools({"token": NATIVE})
        assert result["pool_count"] == len(expected)
        assert {
            row["pool_id"]: int(row["pool_key"]["tick_spacing"])
            for row in result["pools"]
        } == expected
        assert all(
            row["pool_key"]["currency0"] == NATIVE
            and row["pool_key"]["fee_raw"] == str(fee)
            and row["pool_key"]["hooks"] == hook
            for row in result["pools"]
        )
        assert result["coverage"]["catalog"]["omitted_records"] == 1
        assert result["coverage"]["catalog"]["omission_reasons"] == [
            "invalid_v4_configured_fee",
        ]
    finally:
        api.close()
        store.close()


def test_changed_pinned_hash_is_never_published_or_cached():
    v3 = "0x" + "b1" * 20
    store = MarketStore(":memory:")
    _insert(store, [_pool(v3, "v3", TOKEN, HIGH, fee=500, spacing=10)])
    rpc = SnapshotRpc({
        (v3, LIQUIDITY_SELECTOR): abi_encode(["uint128"], [42]),
    }, reorg=True)
    service = Service(store, rpc)
    api = PublicMarketAPI(service)
    try:
        with pytest.raises(PublicAPIReorg, match="changed before publication"):
            api.pools({"token": TOKEN})
        assert not api._cache
    finally:
        api.close()
        store.close()
