from __future__ import annotations

from eth_abi import encode
from eth_utils import keccak

from rhpools.lp_market_protocols import (
    POOL_MANAGER,
    POSITIONS_SELECTOR,
    STATE_VIEW,
    STATE_VIEW_POSITION_SELECTOR,
    V3_MINT_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
    core_position_key,
    decode_logs,
    position_state_requests,
)


def _topic(value: int) -> str:
    return "0x" + f"{value & ((1 << 256) - 1):064x}"


def _log(
    *,
    address: str,
    block_hash: str,
    tx_hash: str,
    tx_index: int,
    topics: list[str],
    data: str,
) -> dict:
    return {
        "address": address,
        "blockNumber": "0xa",
        "blockHash": block_hash,
        "transactionHash": tx_hash,
        "transactionIndex": hex(tx_index),
        "logIndex": "0x0",
        "topics": topics,
        "data": data,
    }


def test_v3_core_source_identity_is_pool_scoped_but_state_key_stays_raw():
    owner = "0x" + "12" * 20
    pool_a = "0x" + "34" * 20
    pool_b = "0x" + "56" * 20
    block_hash = "0x" + "78" * 32
    lower, upper = -60, 60
    raw_key = "0x" + keccak(
        bytes.fromhex(owner[2:])
        + lower.to_bytes(3, "big", signed=True)
        + upper.to_bytes(3, "big", signed=True)
    ).hex()
    topics = [
        V3_MINT_TOPIC,
        _topic(int(owner, 16)),
        _topic(lower),
        _topic(upper),
    ]
    data = "0x" + encode(
        ["address", "uint128", "uint256", "uint256"],
        [owner, 100, 25, 50],
    ).hex()
    rows = decode_logs(
        [
            _log(
                address=pool_a,
                block_hash=block_hash,
                tx_hash="0x" + "9a" * 32,
                tx_index=0,
                topics=topics,
                data=data,
            ),
            _log(
                address=pool_b,
                block_hash=block_hash,
                tx_hash="0x" + "bc" * 32,
                tx_index=1,
                topics=topics,
                data=data,
            ),
        ],
        {
            pool_a: {"id": pool_a, "address": pool_a, "protocol": "v3"},
            pool_b: {"id": pool_b, "address": pool_b, "protocol": "v3"},
        },
        {10: {"hash": block_hash, "timestamp": 1_000}},
    )

    by_pool = {row["pool_id"]: row for row in rows}
    assert by_pool[pool_a]["position_key"] == f"v3:{pool_a}:{raw_key}"
    assert by_pool[pool_b]["position_key"] == f"v3:{pool_b}:{raw_key}"
    assert by_pool[pool_a]["position_key"] != by_pool[pool_b]["position_key"]
    assert {row["data"]["core_position_key"] for row in rows} == {raw_key}
    assert core_position_key("V3", pool_a.upper(), raw_key.upper()) == (
        f"v3:{pool_a}:{raw_key}"
    )

    requests = [
        request
        for request in position_state_requests(rows)
        if request["correlation"]["decoder"] == "v3_core_position"
    ]
    calldata = POSITIONS_SELECTOR + raw_key[2:]
    assert {
        (
            request["params"][0]["to"],
            request["params"][0]["data"],
            request["params"][1],
        )
        for request in requests
    } == {
        (pool_a, calldata, "0x9"),
        (pool_a, calldata, "0xa"),
        (pool_b, calldata, "0x9"),
        (pool_b, calldata, "0xa"),
    }


def test_v4_identical_core_hashes_in_different_pools_keep_distinct_source_keys():
    owner = "0x" + "12" * 20
    pool_a = "0x" + "34" * 32
    pool_b = "0x" + "56" * 32
    block_hash = "0x" + "78" * 32
    salt = "0x" + "9a" * 32
    lower, upper = -60, 60
    raw_key = "0x" + keccak(
        bytes.fromhex(owner[2:])
        + lower.to_bytes(3, "big", signed=True)
        + upper.to_bytes(3, "big", signed=True)
        + bytes.fromhex(salt[2:])
    ).hex()
    data = "0x" + encode(
        ["int24", "int24", "int256", "bytes32"],
        [lower, upper, 100, bytes.fromhex(salt[2:])],
    ).hex()
    rows = decode_logs(
        [
            _log(
                address=POOL_MANAGER,
                block_hash=block_hash,
                tx_hash="0x" + "bc" * 32,
                tx_index=0,
                topics=[
                    V4_MODIFY_LIQUIDITY_TOPIC,
                    pool_a,
                    _topic(int(owner, 16)),
                ],
                data=data,
            ),
            _log(
                address=POOL_MANAGER,
                block_hash=block_hash,
                tx_hash="0x" + "de" * 32,
                tx_index=1,
                topics=[
                    V4_MODIFY_LIQUIDITY_TOPIC,
                    pool_b,
                    _topic(int(owner, 16)),
                ],
                data=data,
            ),
        ],
        {
            pool_a: {"id": pool_a, "address": POOL_MANAGER, "protocol": "v4"},
            pool_b: {"id": pool_b, "address": POOL_MANAGER, "protocol": "v4"},
        },
        {10: {"hash": block_hash, "timestamp": 1_000}},
    )

    by_pool = {row["pool_id"]: row for row in rows}
    assert by_pool[pool_a]["position_key"] == f"v4:{pool_a}:{raw_key}"
    assert by_pool[pool_b]["position_key"] == f"v4:{pool_b}:{raw_key}"
    assert by_pool[pool_a]["position_key"] != by_pool[pool_b]["position_key"]
    assert {row["data"]["core_position_key"] for row in rows} == {raw_key}

    requests = [
        request
        for request in position_state_requests(rows)
        if request["correlation"]["decoder"] == "v4_state_view_position"
    ]
    assert {
        (
            request["params"][0]["to"],
            request["params"][0]["data"],
            request["params"][1],
        )
        for request in requests
    } == {
        (
            STATE_VIEW,
            STATE_VIEW_POSITION_SELECTOR + pool_a[2:] + raw_key[2:],
            "0x9",
        ),
        (
            STATE_VIEW,
            STATE_VIEW_POSITION_SELECTOR + pool_a[2:] + raw_key[2:],
            "0xa",
        ),
        (
            STATE_VIEW,
            STATE_VIEW_POSITION_SELECTOR + pool_b[2:] + raw_key[2:],
            "0x9",
        ),
        (
            STATE_VIEW,
            STATE_VIEW_POSITION_SELECTOR + pool_b[2:] + raw_key[2:],
            "0xa",
        ),
    }
