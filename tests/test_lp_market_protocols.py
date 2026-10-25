from __future__ import annotations

from eth_abi import encode
from eth_utils import keccak

from rhpools.lp_market_protocols import (
    LIQUIDITY_SELECTOR,
    NFT_INCREASE_LIQUIDITY_TOPIC,
    POOL_MANAGER,
    POSITIONS_SELECTOR,
    SLOT0_SELECTOR,
    STATE_VIEW,
    STATE_VIEW_POSITION_SELECTOR,
    UNISWAP_V3_POSITION_MANAGER,
    V2_SWAP_TOPIC,
    V3_FLASH_TOPIC,
    V3_MINT_TOPIC,
    V3_SWAP_TOPIC,
    V4_MODIFY_LIQUIDITY_TOPIC,
    V4_POSITION_MANAGER,
    V4_SWAP_TOPIC,
    core_position_key,
    decode_logs,
    decode_position_state_results,
    nft_position_key,
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

def test_unresolved_v3_factory_pool_state_marks_gap_without_failing_batch():
    owner = "0x" + "12" * 20
    pool = "0x" + "34" * 20
    block_hash = "0x" + "78" * 32
    lower, upper = -60, 60
    topics = [
        V3_MINT_TOPIC,
        _topic(int(owner, 16)),
        _topic(lower),
        _topic(upper),
    ]
    data = "0x" + encode(
        ["address", "uint128", "uint256", "uint256"], [owner, 100, 25, 50],
    ).hex()
    rows = decode_logs(
        [
            _log(
                address=pool,
                block_hash=block_hash,
                tx_hash="0x" + "9a" * 32,
                tx_index=0,
                topics=topics,
                data=data,
            )
        ],
        # Census-catalog pools are identity-verified without a proven factory.
        {pool: {"id": pool, "address": pool, "protocol": "v3", "factory": None}},
        {10: {"hash": block_hash, "timestamp": 1_000}},
    )

    def words(*values: int) -> str:
        return "0x" + "".join(f"{value & ((1 << 256) - 1):064x}" for value in values)

    requests = position_state_requests(rows)
    results = [
        words(0, 0, 0, 0, 0)
        if request["params"][0]["data"].startswith(POSITIONS_SELECTOR)
        else words(1 << 96, 60, 0, 1, 1, 0, 1)
        if request["params"][0]["data"].startswith(SLOT0_SELECTOR)
        else words(123)
        for request in requests
    ]

    updates = decode_position_state_results(requests, results)

    data_update = updates[0]["data"]
    assert "position_before" in data_update and "position_after" in data_update
    state = data_update["pool_state_before"]
    assert state["pinned_block"] == 9
    assert state["source"] == "factory_unresolved"
    assert state["liquidity"] == "123"
    assert "sqrt_price_x96" not in state and "tick" not in state


def test_decoder_does_not_store_unread_data_keys():
    """Swap outputs, the swap fee, flash payments and the NFT position key all
    live in event columns already; no reader uses their ``data`` copies."""
    block_hash = "0x" + "78" * 32
    sender = "0x" + "12" * 20
    recipient = "0x" + "13" * 20
    v2_pool = "0x" + "21" * 20
    v3_pool = "0x" + "31" * 20
    v4_pool = "0x" + "41" * 32

    def log(address, tx, log_index, topics, types, values):
        return {
            "address": address, "blockNumber": "0xa", "blockHash": block_hash,
            "transactionHash": "0x" + f"{tx:064x}", "transactionIndex": hex(tx),
            "logIndex": hex(log_index), "topics": topics,
            "data": "0x" + encode(types, values).hex(),
        }

    lower, upper = -60, 60
    logs = [
        log(v2_pool, 1, 0, [V2_SWAP_TOPIC, _topic(int(sender, 16)), _topic(int(recipient, 16))],
            ["uint256"] * 4, [100, 0, 0, 90]),
        log(v3_pool, 2, 1, [V3_SWAP_TOPIC, _topic(int(sender, 16)), _topic(int(recipient, 16))],
            ["int256", "int256", "uint160", "uint128", "int24"], [100, -90, 1 << 96, 5, 0]),
        log(v3_pool, 3, 2, [V3_FLASH_TOPIC, _topic(int(sender, 16)), _topic(int(recipient, 16))],
            ["uint256"] * 4, [1000, 2000, 3, 6]),
        log(POOL_MANAGER, 4, 3, [V4_SWAP_TOPIC, v4_pool, _topic(int(sender, 16))],
            ["int128", "int128", "uint160", "uint128", "int24", "uint24"],
            [-100, 90, 1 << 96, 5, 0, 3000]),
        log(POOL_MANAGER, 5, 4,
            [V4_MODIFY_LIQUIDITY_TOPIC, v4_pool, _topic(int(V4_POSITION_MANAGER, 16))],
            ["int24", "int24", "int256", "bytes32"],
            [lower, upper, 100, (77).to_bytes(32, "big")]),
        log(v3_pool, 6, 5,
            [V3_MINT_TOPIC, _topic(int(UNISWAP_V3_POSITION_MANAGER, 16)), _topic(lower), _topic(upper)],
            ["address", "uint128", "uint256", "uint256"],
            [UNISWAP_V3_POSITION_MANAGER, 100, 25, 50]),
        log(UNISWAP_V3_POSITION_MANAGER, 6, 6, [NFT_INCREASE_LIQUIDITY_TOPIC, _topic(88)],
            ["uint128", "uint256", "uint256"], [100, 25, 50]),
    ]
    rows = decode_logs(
        logs,
        {
            v2_pool: {"id": v2_pool, "address": v2_pool, "protocol": "v2", "fee_ppm": 3000},
            v3_pool: {"id": v3_pool, "address": v3_pool, "protocol": "v3", "fee_ppm": 3000},
            v4_pool: {"id": v4_pool, "address": POOL_MANAGER, "protocol": "v4"},
        },
        {10: {"hash": block_hash, "timestamp": 1_000}},
    )

    by_tx = {int(row["tx_hash"], 16): row for row in rows}
    assert set(by_tx) == {1, 2, 3, 4, 5, 6}
    assert [by_tx[tx]["kind"] for tx in (1, 2, 3, 4, 5, 6)] == [
        "swap", "swap", "fee", "swap", "add", "add",
    ]
    # The columns that replace the dropped data copies still carry the values.
    assert by_tx[3]["fee_amount0"] == "3" and by_tx[3]["fee_amount1"] == "6"
    assert by_tx[4]["fee_ppm"] == 3000
    assert by_tx[5]["position_key"] == nft_position_key(V4_POSITION_MANAGER, 77)
    assert by_tx[6]["position_key"] == nft_position_key(UNISWAP_V3_POSITION_MANAGER, 88)
    dead = {"output0", "output1", "swap_fee_ppm", "paid0", "paid1", "nft_position_key"}
    for tx, row in by_tx.items():
        assert dead.isdisjoint(row["data"]), (tx, sorted(dead & set(row["data"])))
