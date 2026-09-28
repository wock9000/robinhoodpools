from __future__ import annotations

import contextlib
import sqlite3
import json
from dataclasses import replace

import pytest
from eth_abi import encode

from rhpools import tx_allowlist
from rhpools import tx_chain as tc
from rhpools.tx_chain import (
    ADDRESS_THIS, MSG_SENDER, NATIVE, PERMIT2, PONS_HOOK, POOL_MANAGER, POSM, UR, UR_V2_FACTORY,
    UR_V3_FACTORY, USDG, WETH, NfpmMint, PayPortion, Permit2Permit, PoolKey, PosmMint, Sweep,
    UnwrapWeth, V3Swap,
)
from rhpools.lp_market_protocols import V4_INITIALIZE_TOPIC, _normalize_pool_creation
from rhpools.tx_core import SwapQuote, TxCore
from rhpools.lp_math import sqrt_ratio_at_tick
from rhpools.tx_plan import (
    Amounts, Fee, FeeLeg, Ledger, LpIntent, LpOp, LpPlanner, LpShape, Signatures, SwapIntent,
    SwapPlanner, TxError, TxPolicy, TxRefusal, impact_bps, liquidity_for_amounts, swap_amounts,
)
from rhpools.tx_routes import DEFAULT_BRIDGES, Hop, HookPolicy, IncompletePool, Pool, Route, RouteBook, Side, Venue, v4_pool

WALLET = "0x4444444444444444444444444444444444444444"
FEE_TO = "0x00000000000000000000000000000000000fee75"
PIPEDOG = "0x5cb6f181081301b44905f3ae15419112ecabd8a6"
ITH = "0xb3377f953994c6fa04383a64d88aa8122e81fd62"
V3_POOL = "0xb7f10f74b39291b9290b779978e19a7637c742d6"
V2_POOL = "0x3416b8d8aa6ae642ffdc2f65165ff479d4bd3007"
PONS_KEY = PoolKey(NATIVE, ITH, 0, 200, PONS_HOOK)
PONS = v4_pool(PONS_KEY)
HOOKLESS_ITH = v4_pool(PoolKey(NATIVE, ITH, 500, 10, NATIVE))
BLOCKED_ITH = v4_pool(PoolKey(NATIVE, ITH, 3000, 60, "0x4e3468951d49f2eea976ed0d6e75ffcb44a9a544"))
NATIVE_TRACE = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
POLICY = TxPolicy(fee_bps=75, fee_recipient=FEE_TO)
BLOCK_HASH = "0x" + "ab" * 32
ROWS = [
    (PONS.id, "v4", POOL_MANAGER, NATIVE, ITH, 0, 200, PONS_HOOK, POOL_MANAGER),
    (HOOKLESS_ITH.id, "v4", POOL_MANAGER, NATIVE, ITH, 500, 10, NATIVE, POOL_MANAGER),
    (BLOCKED_ITH.id, "v4", POOL_MANAGER, NATIVE, ITH, 3000, 60, BLOCKED_ITH.hook, POOL_MANAGER),
    (V3_POOL, "v3", V3_POOL, WETH, PIPEDOG, 10000, 200, None, UR_V3_FACTORY),
    ("0x" + "31" * 20, "v3", "0x" + "31" * 20, WETH, PIPEDOG, 3000, 60, None, "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"),
    (V2_POOL, "v2", V2_POOL, WETH, PIPEDOG, None, None, None, UR_V2_FACTORY),
    ("0x" + "32" * 20, "v2", "0x" + "32" * 20, PIPEDOG, USDG, None, None, None, "0x0d1ebb179cdbca88d74c923c4255cb2b17474afd"),
]


def _word(value: int) -> str:
    return f"{value & ((1 << 256) - 1):064x}"


def _topic_addr(address: str) -> str:
    return "0x" + address[2:].rjust(64, "0")


def transfer(token: str, frm: str, to: str, amount: int) -> dict:
    return {"address": token, "topics": [tc.TOPIC_TRANSFER, _topic_addr(frm), _topic_addr(to)], "data": "0x" + _word(amount)}


def v3_swap_log(pool: str, amount0: int, amount1: int) -> dict:
    return {"address": pool, "topics": [tc.TOPIC_V3_SWAP, _topic_addr(UR), _topic_addr(WALLET)], "data": "0x" + _word(amount0) + _word(amount1) + _word(0) * 3}


def v2_swap_log(pool: str, a0_in: int, a1_in: int, a0_out: int, a1_out: int) -> dict:
    return {"address": pool, "topics": [tc.TOPIC_V2_SWAP, _topic_addr(UR), _topic_addr(WALLET)], "data": "0x" + _word(a0_in) + _word(a1_in) + _word(a0_out) + _word(a1_out)}


def v4_swap_log(pool_id: str, amount0: int, amount1: int) -> dict:
    return {"address": POOL_MANAGER, "topics": [tc.TOPIC_V4_SWAP, pool_id, _topic_addr(UR)], "data": "0x" + _word(amount0) + _word(amount1) + _word(0) * 4}


@pytest.fixture
def pools_db(tmp_path):
    path = tmp_path / "pools.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE pools(id TEXT PRIMARY KEY, protocol TEXT NOT NULL, address TEXT NOT NULL, token0 TEXT NOT NULL, token1 TEXT NOT NULL,"
        " fee_ppm INTEGER, tick_spacing INTEGER, hook TEXT, factory TEXT, metadata_json TEXT);"
        "CREATE TABLE lp_pool_state(pool_id TEXT PRIMARY KEY, block_number INTEGER NOT NULL);"
    )
    connection.executemany("INSERT INTO pools VALUES (?,?,?,?,?,?,?,?,?,NULL)", ROWS)
    connection.execute("INSERT INTO lp_pool_state VALUES (?, ?)", (V2_POOL, 500))
    connection.execute("INSERT INTO lp_pool_state VALUES (?, ?)", (V3_POOL, 900))
    connection.commit()
    connection.close()

    @contextlib.contextmanager
    def reader():
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            yield connection
        finally:
            connection.close()

    return reader


class FakeRPC:
    def __init__(self) -> None:
        self.block = 100
        self.block_hash = BLOCK_HASH
        self.balances: dict[tuple[str, str], int] = {(NATIVE, WALLET): 10**20, (USDG, WALLET): 10**12, (ITH, WALLET): 10**24}
        self.allowances: dict[tuple[str, str, str], int] = {}
        self.permit_nonces: dict[str, int] = {}
        self.code: dict[str, str] = {a: "0x60" + a[-2:] for a in tx_allowlist.CODE_HASHES}
        self.launches = [1] * 13
        self.launches[7], self.launches[10] = 200, 100
        self.simulate = None
        self.estimate = 150_000
        self.calls: list[tuple[str, list]] = []
        self.txs: dict[str, dict] = {}
        self.receipts: dict[str, dict] = {}

    def pins(self) -> dict[str, str]:
        return {a: tx_allowlist.code_hash(c) for a, c in self.code.items()}

    def call(self, method, params):
        self.calls.append((method, params))
        if method == "eth_chainId":
            return hex(4663)
        if method == "eth_getBlockByNumber":
            requested = params[0]
            number = self.block if requested == "latest" else int(requested, 16)
            return {"number": hex(number), "hash": self.block_hash if number == self.block else "0x" + "cd" * 32}
        if method == "eth_getCode":
            return self.code.get(params[0].lower(), "0x")
        if method == "eth_getBalance":
            return hex(self.balances.get((NATIVE, params[0].lower()), 0))
        if method == "eth_simulateV1":
            assert self.simulate is not None, "no simulation scripted"
            self.last_state = params[0]["blockStateCalls"][0]
            calls = self.last_state["calls"]
            outs = [{"status": "0x1", "logs": [], "gasUsed": "0x5208"} for _ in calls[:-1]]
            outs.append(self.simulate(calls[-1], params[1]))
            return [{"calls": outs}]
        if method == "eth_estimateGas":
            return hex(self.estimate)
        if method == "eth_getTransactionByHash":
            return self.txs.get(params[0])
        if method == "eth_getTransactionReceipt":
            return self.receipts.get(params[0])
        if method != "eth_call":
            raise AssertionError(f"unexpected RPC method {method}")
        request = params[0]
        target = request["to"].lower()
        data = bytes.fromhex(request["data"][2:])
        sel = data[:4]
        if sel == tc.SEL_ERC20_BALANCE_OF:
            owner = "0x" + data[16:36].hex()
            return "0x" + _word(self.balances.get((target, owner), 0))
        if sel == tc.SEL_ERC20_ALLOWANCE:
            owner, spender = "0x" + data[16:36].hex(), "0x" + data[48:68].hex()
            return "0x" + _word(self.allowances.get((target, owner, spender), 0))
        if target == PERMIT2 and sel == tc.SEL_PERMIT2_ALLOWANCE:
            token = "0x" + data[48:68].hex()
            return "0x" + _word(0) + _word(0) + _word(self.permit_nonces.get(token, 0))
        if target == PONS_HOOK and sel == tc.SEL_PONS_LAUNCHES:
            return "0x" + "".join(_word(w) for w in self.launches)
        if target == tc.STATE_VIEW and sel == tc.SEL_STATE_GET_SLOT0:
            return "0x" + _word(1 << 96) + _word(0) * 3
        raise AssertionError(f"unexpected eth_call target={target} selector={sel.hex()}")


@pytest.fixture
def rpc(monkeypatch):
    rpc = FakeRPC()
    monkeypatch.setattr(tx_allowlist, "CODE_HASHES", rpc.pins())
    return rpc


@pytest.fixture
def core(rpc, pools_db):
    clock = {"now": 1_000_000}
    core = TxCore(rpc, RouteBook(pools_db, rpc), POLICY, clock=lambda: clock["now"])
    core.test_clock = clock
    return core


def buy_intent(**kw) -> SwapIntent:
    base = dict(wallet=WALLET, side=Side.BUY, token=PIPEDOG, quote_currency=NATIVE, amount_in=10**18, slippage_bps=100)
    base.update(kw)
    return SwapIntent(**base)


def v3_buy_simulation(amount_out: int):
    def simulate(call, block):
        value = int(call["value"], 16)
        fee = value * 75 // 10_000
        swapped = value - fee
        out = amount_out if swapped == 10**18 - 10**18 * 75 // 10_000 else amount_out // 100
        logs = [
            transfer(NATIVE_TRACE, WALLET, UR, value),
            transfer(NATIVE_TRACE, UR, FEE_TO, fee),
            v3_swap_log(V3_POOL, swapped, -out),
            transfer(PIPEDOG, V3_POOL, WALLET, out),
        ]
        return {"status": "0x1", "logs": logs, "gasUsed": hex(150_000)}

    return simulate


def usdg_buy_simulation(amount_out: int):
    """The V2 bridge candidate reverts."""

    def simulate(call, block):
        commands, _ = tc.decode_ur_execute(bytes.fromhex(call["data"][2:]))
        if any(type(c).__name__ == "V2Swap" for c in commands):
            return {"status": "0x0", "logs": [], "gasUsed": "0x10", "returnData": "0x" + tc.selector("V2TooLittleReceived()").hex()}
        amount_in = commands[0].amount
        fee = amount_in * 75 // 10_000
        swapped = amount_in - fee
        weth = swapped * 10**9
        out = amount_out * amount_in // 10**9
        logs = [
            transfer(USDG, WALLET, UR, amount_in),
            transfer(USDG, UR, FEE_TO, fee),
            v3_swap_log(DEFAULT_BRIDGES[0].address, -weth, swapped),
            transfer(WETH, DEFAULT_BRIDGES[0].address, UR, weth),
            v3_swap_log(V3_POOL, weth, -out),
            transfer(PIPEDOG, V3_POOL, WALLET, out),
        ]
        return {"status": "0x1", "logs": logs, "gasUsed": hex(250_000)}

    return simulate


def test_route_book_filters_and_orders_candidates(pools_db, rpc):
    routes = RouteBook(pools_db, rpc)
    buy = routes.candidates(PIPEDOG, NATIVE, Side.BUY)
    assert [[h.pool.id for h in r.hops] for r in buy] == [[V3_POOL], [V2_POOL], ["0x" + "31" * 20]]
    assert buy[0].hops[0] == Hop(routes.pool(V3_POOL), WETH, PIPEDOG)
    sell = routes.candidates(PIPEDOG, USDG, Side.SELL)
    assert all(r.hops[0].currency_in == PIPEDOG and r.hops[-1].currency_out == USDG for r in sell)
    assert {r.hops[1].pool.id for r in sell} == {DEFAULT_BRIDGES[i].id for i in (0, 4, -1)}
    assert len(sell) == 6
    pons = routes.candidates(ITH, NATIVE, Side.BUY)
    assert [h.pool.hook for r in pons for h in r.hops] == [PONS_HOOK, NATIVE]
    assert routes.candidates(WETH, USDG, Side.BUY) == []
    assert routes.candidates(PIPEDOG, PIPEDOG, Side.BUY) == []


def test_route_book_hook_policy_is_cached_and_unknown_hooks_dropped(pools_db, rpc):
    routes = RouteBook(pools_db, rpc)
    policy = routes.hook_policy(PONS)
    assert policy == HookPolicy(hook_fee_bps=100, creator_tax_bps=200)
    calls = len(rpc.calls)
    assert routes.hook_policy(PONS) is policy and len(rpc.calls) == calls
    rpc.launches[0] = 0
    fresh = RouteBook(pools_db, rpc)
    assert [h.pool.hook for r in fresh.candidates(ITH, NATIVE, Side.BUY) for h in r.hops] == [NATIVE]


def test_route_book_drops_v4_rows_whose_id_is_not_the_key_hash(rpc, tmp_path):
    path = tmp_path / "bad.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript("CREATE TABLE pools(id TEXT PRIMARY KEY, protocol TEXT, address TEXT, token0 TEXT, token1 TEXT, fee_ppm INTEGER, tick_spacing INTEGER, hook TEXT, factory TEXT, metadata_json TEXT); CREATE TABLE lp_pool_state(pool_id TEXT PRIMARY KEY, block_number INTEGER);")
    connection.execute("INSERT INTO pools VALUES (?,?,?,?,?,?,?,?,?,NULL)", ("0x" + "99" * 32, "v4", POOL_MANAGER, NATIVE, ITH, 0, 200, PONS_HOOK, POOL_MANAGER))
    connection.commit()
    connection.close()

    @contextlib.contextmanager
    def reader():
        c = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            yield c
        finally:
            c.close()

    routes = RouteBook(reader, rpc)
    with pytest.raises(IncompletePool, match="PoolKey identity"):
        routes.pool("0x" + "99" * 32)
    assert routes.candidates(ITH, NATIVE, Side.BUY) == []


def test_real_initialize_dynamic_fee_key_survives_storage_and_route_decode(pools_db, rpc):
    pool_id = "0x90fc4fa48f86bca27b5da6a050361a51c87483a6c5bdb631a235e771f4c167b6"
    log = {
        "address": POOL_MANAGER, "blockNumber": "0x46d3fbe",
        "topics": [
            V4_INITIALIZE_TOPIC, pool_id,
            "0x0000000000000000000000002e8c31162b855a2ffa90f6f8634643ad6f111e18",
            "0x0000000000000000000000003521b8a7de164723c6c51aa80d106fa852111e18",
        ],
        "data": (
            "0x0000000000000000000000000000000000000000000000000000000000800000"
            "0000000000000000000000000000000000000000000000000000000000000008"
            "0000000000000000000000004e3468951d49f2eea976ed0d6e75ffcb44a9a544"
            "0000000000000000000000000000000000000066928603d12b55b292b1cf6f1b"
            "00000000000000000000000000000000000000000000000000000000000169c8"
        ),
    }
    decoded = _normalize_pool_creation(log, V4_INITIALIZE_TOPIC)
    assert decoded["id"] == pool_id
    assert decoded["fee_ppm"] is None
    with pools_db() as reader:
        path = reader.execute("PRAGMA database_list").fetchone()[2]
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO pools VALUES (?,?,?,?,?,?,?,?,?,?)",
            (decoded["id"], decoded["protocol"], decoded["address"], decoded["token0"],
             decoded["token1"], decoded["fee_ppm"], decoded["tick_spacing"], decoded["hook"],
             decoded["factory"], json.dumps(decoded["metadata_json"])),
        )
    pool = RouteBook(pools_db, rpc).pool(pool_id)
    assert pool.fee_ppm == 0x800000
    assert pool.key.id() == pool_id


def test_route_book_decodes_dynamic_fee_and_refuses_incomplete_spacing(pools_db, rpc):
    dynamic = v4_pool(PoolKey(NATIVE, ITH, 0x800000, 10, NATIVE))
    incomplete = v4_pool(PoolKey(NATIVE, PIPEDOG, 500, 10, NATIVE))
    with pools_db() as reader:
        path = reader.execute("PRAGMA database_list").fetchone()[2]
    with sqlite3.connect(path) as connection:
        connection.execute(
            "INSERT INTO pools VALUES (?,?,?,?,?,?,?,?,?,?)",
            (dynamic.id, "v4", POOL_MANAGER, NATIVE, ITH, None, 10, NATIVE, POOL_MANAGER,
             '{"configured_fee":8388608,"dynamic_fee":true}'),
        )
        connection.execute(
            "INSERT INTO pools VALUES (?,?,?,?,?,?,?,?,?,?)",
            (incomplete.id, "v4", POOL_MANAGER, NATIVE, PIPEDOG, 500, None, NATIVE, POOL_MANAGER,
             '{"configured_fee":500,"dynamic_fee":false}'),
        )
    routes = RouteBook(pools_db, rpc)
    assert routes.pool(dynamic.id).key.id() == dynamic.id
    core = TxCore(rpc, routes, POLICY)
    with pytest.raises(TxRefusal, match="tick_spacing") as view_refusal:
        core.pool_view(incomplete.id, WALLET)
    assert view_refusal.value.code == "incomplete_pool"
    intent = LpIntent(
        wallet=WALLET, op=LpOp.MINT, pool_id=incomplete.id, slippage_bps=50,
        tick_lower=-100, tick_upper=100, amount0=10**18, amount1=10**18,
    )
    with pytest.raises(TxRefusal, match="tick_spacing") as quote_refusal:
        core.quote(intent)
    assert quote_refusal.value.code == "incomplete_pool"
    assert all(pool.id != incomplete.id for pool in routes.quote_pools(PIPEDOG))

def test_route_rejects_non_contiguous_hops():
    v3 = Pool(Venue.V3, V3_POOL, V3_POOL, WETH, PIPEDOG, 10000, 200, NATIVE, UR_V3_FACTORY)
    with pytest.raises(ValueError):
        Route((Hop(v3, WETH, PIPEDOG), Hop(PONS, NATIVE, ITH)))
    with pytest.raises(ValueError):
        Route((Hop(v3, USDG, PIPEDOG),))
    Route((Hop(PONS, ITH, NATIVE), Hop(DEFAULT_BRIDGES[0], WETH, USDG)))


def test_min_out_lives_in_the_last_swap_for_input_fee_plans():
    v3 = Pool(Venue.V3, V3_POOL, V3_POOL, WETH, PIPEDOG, 10000, 200, NATIVE, UR_V3_FACTORY)
    plan = SwapPlanner().plan(buy_intent(), Route((Hop(v3, WETH, PIPEDOG),)), POLICY, 5_000, 0)
    kinds = [type(c).__name__ for c in plan.body.commands]
    assert kinds == ["PayPortion", "WrapEth", "V3Swap"]
    assert plan.body.commands[0] == PayPortion(NATIVE, FEE_TO, 75)
    assert plan.value == 10**18 and plan.permit is None and plan.approvals == () and plan.staging == ()
    final = SwapPlanner().finalize(plan, 123, Signatures())
    swap = final.body.commands[final.body.min_out_index]
    assert isinstance(swap, V3Swap) and swap.min_out == 123 and swap.recipient == MSG_SENDER
    assert plan.shape.fee_leg is FeeLeg.INPUT and plan.shape.fee_currency == NATIVE


def test_min_out_lives_in_sweep_or_unwrap_for_output_fee_plans():
    v3 = Pool(Venue.V3, V3_POOL, V3_POOL, WETH, PIPEDOG, 10000, 200, NATIVE, UR_V3_FACTORY)
    sell = buy_intent(side=Side.SELL, amount_in=5_000)
    plan = SwapPlanner().plan(sell, Route((Hop(v3, PIPEDOG, WETH),)), POLICY, 5_000, 7)
    kinds = [type(c).__name__ for c in plan.body.commands]
    assert kinds == ["Permit2TransferFrom", "V3Swap", "PayPortion", "UnwrapWeth"]
    assert plan.permit.details.nonce == 7 and plan.permit.details.amount == 5_000 and plan.permit.sig_deadline == 5_000
    assert [c.to for c in plan.staging] == [PIPEDOG, PERMIT2]
    assert plan.body.commands[1].recipient == ADDRESS_THIS and plan.shape.fee_currency == WETH
    with pytest.raises(TxError) as err:
        SwapPlanner().finalize(plan, 1, Signatures())
    assert err.value.code == "permit_required"
    final = SwapPlanner().finalize(plan, 99, Signatures(permit=b"\x01" * 65))
    decoded, _ = tc.decode_ur_execute(final.calldata())
    assert isinstance(decoded[0], Permit2Permit) and decoded[0].signature == b"\x01" * 65
    assert decoded[-1] == UnwrapWeth(MSG_SENDER, 99)
    usdg_sell = buy_intent(side=Side.SELL, quote_currency=USDG, amount_in=5_000)
    plan = SwapPlanner().plan(usdg_sell, Route((Hop(v3, PIPEDOG, WETH), Hop(DEFAULT_BRIDGES[0], WETH, USDG))), POLICY, 5_000, 0)
    assert isinstance(plan.body.commands[-1], Sweep) and plan.body.commands[-1].token == USDG
    weth_sell = buy_intent(side=Side.SELL, quote_currency=WETH, token=ITH, amount_in=5_000)
    plan = SwapPlanner().plan(weth_sell, Route((Hop(PONS, ITH, NATIVE),)), POLICY, 5_000, 0)
    assert [type(c).__name__ for c in plan.body.commands] == ["Permit2TransferFrom", "V4Swap", "WrapEth", "PayPortion", "Sweep"]


def test_ledger_hop_io_sign_conventions():
    v3 = Pool(Venue.V3, V3_POOL, V3_POOL, WETH, PIPEDOG, 10000, 200, NATIVE, UR_V3_FACTORY)
    v2 = Pool(Venue.V2, V2_POOL, V2_POOL, WETH, PIPEDOG, 0, 0, NATIVE, UR_V2_FACTORY)
    ledger = Ledger([v3_swap_log(V3_POOL, 100, -40), v2_swap_log(V2_POOL, 0, 7, 3, 0), v4_swap_log(PONS.id, -50, 9)], traced=True)
    assert ledger.hop_io(Hop(v3, WETH, PIPEDOG)) == (100, 40)
    assert ledger.hop_io(Hop(v2, PIPEDOG, WETH)) == (7, 3)
    assert ledger.hop_io(Hop(PONS, NATIVE, ITH)) == (50, 9)
    with pytest.raises(TxError):
        ledger.hop_io(Hop(v3, PIPEDOG, WETH)) and Ledger([], traced=True).hop_io(Hop(v3, WETH, PIPEDOG))


def test_swap_amounts_reconciles_pons_floors_and_refuses_one_wei_drift():
    intent = SwapIntent(wallet=WALLET, side=Side.BUY, token=ITH, quote_currency=NATIVE, amount_in=10**16, slippage_bps=100)
    plan = SwapPlanner().plan(intent, Route((Hop(PONS, NATIVE, ITH),)), POLICY, 5_000, 0)
    fee = 10**16 * 75 // 10_000
    pool_out = 123_456_789_012_345
    net = pool_out - pool_out * 100 // 10_000 - pool_out * 200 // 10_000
    logs = [
        transfer(NATIVE_TRACE, WALLET, UR, 10**16),
        transfer(NATIVE_TRACE, UR, FEE_TO, fee),
        v4_swap_log(PONS.id, -(10**16 - fee), pool_out),
        transfer(ITH, POOL_MANAGER, WALLET, net),
    ]
    amounts = swap_amounts(plan.shape, Ledger(logs, traced=True), lambda pool: HookPolicy(100, 200))
    assert (amounts.pool_out, amounts.net_out, amounts.rhpools_fee.amount) == (pool_out, net, fee)
    assert amounts.hook_fee.amount == pool_out * 100 // 10_000 and amounts.creator_tax.currency == ITH
    logs[-1] = transfer(ITH, POOL_MANAGER, WALLET, net - 1)
    with pytest.raises(TxError) as err:
        swap_amounts(plan.shape, Ledger(logs, traced=True), lambda pool: HookPolicy(100, 200))
    assert err.value.code == "unmodeled_fee"
    logs[-1] = transfer(ITH, POOL_MANAGER, WALLET, net)
    logs[1] = transfer(NATIVE_TRACE, UR, FEE_TO, fee + 1)
    with pytest.raises(TxError):
        swap_amounts(plan.shape, Ledger(logs, traced=True), lambda pool: HookPolicy(100, 200))


def test_swap_amounts_output_fee_after_hooked_first_hop():
    intent = SwapIntent(wallet=WALLET, side=Side.SELL, token=ITH, quote_currency=USDG, amount_in=10**18, slippage_bps=100)
    bridge = DEFAULT_BRIDGES[0]
    plan = SwapPlanner().plan(intent, Route((Hop(PONS, ITH, NATIVE), Hop(bridge, WETH, USDG))), POLICY, 5_000, 0)
    pool_out = 3_000_000_000_000_000
    carry = pool_out - pool_out * 100 // 10_000 - pool_out * 200 // 10_000
    usdg_out = 8_000_000
    fee = usdg_out * 75 // 10_000
    logs = [
        transfer(ITH, WALLET, UR, 10**18),
        v4_swap_log(PONS.id, carry + 0, -(10**18)),
        v3_swap_log(bridge.address, carry, -usdg_out),
        transfer(USDG, bridge.address, UR, usdg_out),
        transfer(USDG, UR, FEE_TO, fee),
        transfer(USDG, UR, WALLET, usdg_out - fee),
    ]
    logs[1] = v4_swap_log(PONS.id, pool_out, -(10**18))
    amounts = swap_amounts(plan.shape, Ledger(logs, traced=False), lambda pool: HookPolicy(100, 200) if pool.id == PONS.id else HookPolicy())
    assert amounts.net_out == usdg_out - fee and amounts.rhpools_fee.amount == fee and amounts.hook_fee.currency == NATIVE
    assert amounts.pool_out == usdg_out


def test_impact_bps():
    full = Amounts(10**18, 0, None, None, Fee(NATIVE, 0), 900, 0, None)
    small = replace(full, amount_in=10**16, net_out=10)
    assert impact_bps(full, small) == 1000
    assert impact_bps(full, replace(small, net_out=0)) is None
    assert impact_bps(replace(full, net_out=10**4), small) == 0


def test_quote_prepare_round_trip_and_idempotence(core, rpc):
    rpc.simulate = v3_buy_simulation(4_000_000)
    quote = core.quote(buy_intent())
    assert isinstance(quote, SwapQuote)
    assert [h.pool.id for h in quote.route.hops] == [V3_POOL]
    assert quote.amounts.net_out == 4_000_000 and quote.amounts.min_out == 3_960_000
    assert quote.amounts.rhpools_fee.amount == 10**18 * 75 // 10_000 and quote.amounts.impact_bps == 0
    assert [s.kind for s in quote.steps] == ["send"]
    assert quote.expires_at == 1_000_060 and quote.deadline == 1_000_120 and quote.block.number == 100
    first = core.prepare(quote.quote_id, WALLET, Signatures())
    second = core.prepare(quote.quote_id, WALLET, Signatures())
    assert first.transaction == second.transaction
    tx = first.transaction
    assert tx["to"] == UR and tx["value"] == hex(10**18) and tx["chainId"] == hex(4663) and tx["gas"] == hex(150_000 * 130 // 100)
    commands, deadline = tc.decode_ur_execute(bytes.fromhex(tx["data"][2:]))
    assert deadline == 1_000_120 and commands[-1].min_out == 3_960_000
    assert quote.to_json()["amounts"]["net_out"] == "4000000"


def test_gas_limit_covers_the_node_estimate(core, rpc):
    rpc.simulate = v3_buy_simulation(4_000_000)
    rpc.estimate = 400_000
    quote = core.quote(buy_intent())
    assert core.prepare(quote.quote_id, WALLET, Signatures()).transaction["gas"] == hex(400_000 * 125 // 100)


def test_quote_picks_best_route_and_reports_failures(core, rpc):
    seen = []

    def simulate(call, block):
        data = bytes.fromhex(call["data"][2:])
        if call["to"] == tc.PANCAKE_SMART_ROUTER:
            return {"status": "0x0", "logs": [], "gasUsed": "0x10", "returnData": "0x" + tc.selector("V3TooLittleReceived()").hex()}
        commands, _ = tc.decode_ur_execute(data)
        seen.append(type(commands[-1]).__name__)
        if type(commands[-1]).__name__ == "V2Swap":
            return {"status": "0x0", "logs": [], "gasUsed": "0x10", "returnData": "0x" + tc.selector("V2TooLittleReceived()").hex()}
        return v3_buy_simulation(4_000_000)(call, block)

    rpc.simulate = simulate
    quote = core.quote(buy_intent())
    assert quote.route.hops[0].pool.venue is Venue.V3 and seen.count("V2Swap") == 1
    rpc.simulate = lambda call, block: {"status": "0x0", "logs": [], "gasUsed": "0x10", "error": {"data": "0x" + tc.selector("V3TooLittleReceived()").hex()}}
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent())
    assert refused.value.code == "no_route" and "slippage" in refused.value.detail


def test_underfunded_buy_is_priced_but_not_preparable(core, rpc):
    rpc.balances[(NATIVE, WALLET)] = 10**17
    rpc.simulate = v3_buy_simulation(4_000_000)
    quote = core.quote(buy_intent())
    assert rpc.last_state["stateOverrides"] == {WALLET: {"balance": hex(10**18)}}
    assert quote.amounts.net_out == 4_000_000
    assert quote.to_json()["shortfall"] == {"currency": NATIVE, "have": str(10**17), "need": str(10**18)}
    with pytest.raises(TxRefusal) as refused:
        core.prepare(quote.quote_id, WALLET, Signatures())
    assert refused.value.code == "insufficient_balance"
    rpc.balances[(NATIVE, WALLET)] = 10**20
    assert core.prepare(quote.quote_id, WALLET, Signatures()).transaction["value"] == hex(10**18)


def test_quote_refusals(core, rpc):
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent(quote_currency=USDG, amount_in=10**13))
    assert refused.value.code == "insufficient_balance"
    with pytest.raises(TxError) as err:
        core.quote(buy_intent(token=WETH, quote_currency=NATIVE))
    assert err.value.code == "invalid_intent"
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent(wallet=FEE_TO))
    assert refused.value.code == "fee_wallet"
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent(token="0x" + "ee" * 20))
    assert refused.value.code == "no_route"
    with pytest.raises(TxError) as err:
        core.quote(buy_intent(quote_currency=PIPEDOG))
    assert err.value.code == "invalid_intent"
    with pytest.raises(TxError):
        core.quote(buy_intent(slippage_bps=6000))

    def drifting(call, block):
        out = v3_buy_simulation(4_000_000)(call, block)
        out["logs"][1] = transfer(NATIVE_TRACE, UR, FEE_TO, 1)
        return out

    rpc.simulate = drifting
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent())
    assert refused.value.code == "unmodeled_fee"

    def steep(call, block):
        out = v3_buy_simulation(4_000_000)(call, block)
        if int(call["value"], 16) == 10**18:
            out["logs"][2] = v3_swap_log(V3_POOL, 10**18 - 10**18 * 75 // 10_000, -1_000_000)
            out["logs"][3] = transfer(PIPEDOG, V3_POOL, WALLET, 1_000_000)
        return out

    rpc.simulate = steep
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent())
    assert refused.value.code == "impact_over_limit"


def test_prepare_guards(core, rpc):
    rpc.simulate = v3_buy_simulation(4_000_000)
    quote = core.quote(buy_intent())
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, "0x" + "55" * 20, Signatures())
    assert err.value.code == "wallet_mismatch"
    with pytest.raises(TxError) as err:
        core.prepare("0" * 64, WALLET, Signatures())
    assert err.value.code == "unknown_quote"
    rpc.block_hash = "0x" + "ef" * 32
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, WALLET, Signatures())
    assert err.value.code == "reorg"
    rpc.block_hash = BLOCK_HASH
    rpc.simulate = lambda call, block: {"status": "0x0", "logs": [], "gasUsed": "0x10", "returnData": "0x5bf6f916"}
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, WALLET, Signatures())
    assert err.value.code == "expired"
    rpc.simulate = v3_buy_simulation(4_000_000)
    core.test_clock["now"] = quote.expires_at
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, WALLET, Signatures())
    assert err.value.code == "expired"
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, WALLET, Signatures())
    assert err.value.code == "unknown_quote"


def test_prepare_requires_erc20_allowance_and_permit(core, rpc):
    rpc.simulate = usdg_buy_simulation(4_000_000)
    rpc.permit_nonces[USDG] = 3
    quote = core.quote(buy_intent(quote_currency=USDG, amount_in=10**9))
    assert [s.kind for s in quote.steps] == ["approve", "permit", "send"]
    approve = quote.step("approve").tx
    assert approve["to"] == USDG and bytes.fromhex(approve["data"][2:])[:4] == tc.SEL_ERC20_APPROVE
    typed = quote.step("permit").typed_data
    assert typed["message"]["details"] == {"token": USDG, "amount": str(10**9), "expiration": str(quote.deadline), "nonce": "3"}
    assert typed["message"]["sigDeadline"] == str(quote.deadline) and typed["message"]["spender"] == UR
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, WALLET, Signatures(permit=b"\x01" * 65))
    assert err.value.code == "approve_pending"
    rpc.allowances[(USDG, WALLET, PERMIT2)] = 1 << 200
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, WALLET, Signatures())
    assert err.value.code == "permit_required"
    quote2 = core.quote(buy_intent(quote_currency=USDG, amount_in=10**9))
    assert [s.kind for s in quote2.steps] == ["permit", "send"]


def test_quote_store_ttl_and_cap(rpc, pools_db):
    clock = {"now": 1_000}
    core = TxCore(rpc, RouteBook(pools_db, rpc), POLICY, max_quotes=2, clock=lambda: clock["now"])
    rpc.simulate = v3_buy_simulation(4_000_000)
    a = core.quote(buy_intent())
    b = core.quote(buy_intent())
    c = core.quote(buy_intent())
    assert set(core._quotes) == {b.quote_id, c.quote_id}
    clock["now"] = a.expires_at
    d = core.quote(buy_intent())
    assert set(core._quotes) == {d.quote_id}


def test_allowlist_mismatch_disables_core(rpc, pools_db):
    rpc.code[UR] = "0x6001"
    core = TxCore(rpc, RouteBook(pools_db, rpc), POLICY)
    assert not core.enabled and [m.address for m in core.mismatches] == [UR]
    with pytest.raises(TxRefusal) as refused:
        core.quote(buy_intent())
    assert refused.value.code == "allowlist_mismatch"


def test_receipt_decodes_swap_from_calldata_and_logs(core, rpc):
    rpc.simulate = v3_buy_simulation(4_000_000)
    quote = core.quote(buy_intent())
    tx = core.prepare(quote.quote_id, WALLET, Signatures()).transaction
    tx_hash = "0x" + "11" * 32
    rpc.txs[tx_hash] = {"from": WALLET, "to": UR, "input": tx["data"], "value": tx["value"]}
    pending = core.receipt(tx_hash, WALLET)
    assert pending.status == "pending" and pending.amounts is None
    fee = 10**18 * 75 // 10_000
    rpc.receipts[tx_hash] = {
        "status": "0x1", "blockNumber": hex(101), "gasUsed": hex(140_000), "effectiveGasPrice": hex(10**8),
        "logs": [v3_swap_log(V3_POOL, 10**18 - fee, -3_999_000), transfer(PIPEDOG, V3_POOL, WALLET, 3_999_000)],
    }
    fill = core.receipt(tx_hash, WALLET)
    assert fill.status == "confirmed" and fill.block == 101 and fill.gas_cost_native == 140_000 * 10**8
    assert fill.amounts.net_out == 3_999_000 and fill.amounts.rhpools_fee.amount == fee and fill.amounts.amount_in == 10**18
    assert fill.to_json()["amounts"]["rhpools_fee"] == {"currency": NATIVE, "amount": str(fee)}
    with pytest.raises(TxError) as err:
        core.receipt(tx_hash, "0x" + "55" * 20)
    assert err.value.code == "wallet_mismatch"
    rpc.receipts[tx_hash]["status"] = "0x0"
    assert core.receipt(tx_hash, WALLET).status == "failed"


def test_swap_shape_round_trips_through_calldata(core):
    planner = SwapPlanner()
    v3 = core.routes.pool(V3_POOL)
    v2 = core.routes.pool(V2_POOL)
    cases = [
        (buy_intent(), Route((Hop(v3, WETH, PIPEDOG),))),
        (buy_intent(quote_currency=USDG, amount_in=10**9), Route((Hop(DEFAULT_BRIDGES[0], USDG, WETH), Hop(v2, WETH, PIPEDOG)))),
        (buy_intent(side=Side.SELL, quote_currency=USDG, amount_in=10**9), Route((Hop(v3, PIPEDOG, WETH), Hop(DEFAULT_BRIDGES[0], WETH, USDG)))),
        (SwapIntent(WALLET, Side.SELL, ITH, USDG, 10**18, 50), Route((Hop(PONS, ITH, NATIVE), Hop(DEFAULT_BRIDGES[-1], NATIVE, USDG)))),
        (SwapIntent(WALLET, Side.BUY, ITH, WETH, 10**18, 50), Route((Hop(PONS, NATIVE, ITH),))),
    ]
    for intent, route in cases:
        plan = planner.plan(intent, route, POLICY, 5_000, 0)
        final = planner.finalize(plan, 42, Signatures(permit=b"\x02" * 65))
        assert core._swap_shape(WALLET, final.calldata(), final.value) == plan.shape


def test_lp_planner_bounds_and_permit_batch(core, rpc):
    hookless = v4_pool(PoolKey(NATIVE, ITH, 500, 10, NATIVE))
    intent = LpIntent(wallet=WALLET, op=LpOp.MINT, pool_id=hookless.id, slippage_bps=50, tick_lower=-100, tick_upper=100, amount0=10**18, amount1=10**18)
    plan = LpPlanner().plan(intent, hookless, 5_000, sqrt_price_x96=1 << 96, position=None, permit_nonces={ITH: 4})
    mint = plan.body.params[0]
    assert isinstance(mint, PosmMint)
    assert mint.liquidity == liquidity_for_amounts(1 << 96, sqrt_ratio_at_tick(-100), sqrt_ratio_at_tick(100), 10**18, 10**18) > 0
    assert plan.value == 10**18 and plan.permit.details[0].nonce == 4 and plan.permit.spender == POSM
    assert [type(p).__name__ for p in plan.body.params] == ["PosmMint", "PosmSettlePair", "PosmSweep"]
    with pytest.raises(TxError) as err:
        LpPlanner().finalize(plan, (1, 1), Signatures())
    assert err.value.code == "permit_required"
    final = LpPlanner().finalize(plan, (5, 6), Signatures(permit=b"\x03" * 65))
    calls = tc.decode_multicall(final.calldata())
    assert calls[0][:4] == tc.SEL_POSM_PERMIT_BATCH
    params, deadline = tc.decode_posm_modify_liquidities(calls[1])
    assert deadline == 5_000 and (params[0].amount0_max, params[0].amount1_max) == (5, 6)
    v3 = core.routes.pool(V3_POOL)
    mint_v3 = LpIntent(wallet=WALLET, op=LpOp.MINT, pool_id=V3_POOL, slippage_bps=50, tick_lower=-200, tick_upper=200, amount0=7, amount1=9)
    plan = LpPlanner().plan(mint_v3, v3, 5_000, sqrt_price_x96=1 << 96, position=None, permit_nonces={})
    assert plan.to == tc.NFPM_UNISWAP and [a.token for a in plan.approvals] == [WETH, PIPEDOG] and plan.permit is None
    final = LpPlanner().finalize(plan, (3, 4), Signatures())
    decoded = tc.decode_nfpm_call(final.calldata())
    assert isinstance(decoded, NfpmMint) and (decoded.amount0_min, decoded.amount1_min) == (3, 4)
    with pytest.raises(TxError):
        LpPlanner().plan(replace(mint_v3, tick_lower=-150), v3, 5_000, sqrt_price_x96=1 << 96, position=None, permit_nonces={})


def test_lp_amounts_direction_per_manager():
    hookless = v4_pool(PoolKey(NATIVE, ITH, 500, 10, NATIVE))
    shape = LpShape(WALLET, LpOp.MINT, hookless, POSM, None, -100, 100, 0)
    logs = [
        transfer(NATIVE_TRACE, WALLET, POSM, 10**18),
        {"address": POSM, "topics": [tc.TOPIC_TRANSFER, _topic_addr(NATIVE), _topic_addr(WALLET), "0x" + _word(77)], "data": "0x"},
        {"address": POOL_MANAGER, "topics": [tc.TOPIC_V4_MODIFY_LIQUIDITY, hookless.id, _topic_addr(POSM)], "data": "0x" + _word(-100) + _word(100) + _word(12345) + _word(0)},
        transfer(NATIVE_TRACE, POSM, POOL_MANAGER, 6 * 10**17),
        transfer(ITH, WALLET, POOL_MANAGER, 5 * 10**17),
        transfer(NATIVE_TRACE, POSM, WALLET, 4 * 10**17),
    ]
    amounts = LpPlanner().amounts(shape, Ledger(logs, traced=True), 100, 1 << 96)
    assert (amounts.liquidity, amounts.amount0, amounts.amount1, amounts.token_id) == (12345, 6 * 10**17, 5 * 10**17, 77)
    assert (amounts.bound0, amounts.bound1) == (6 * 10**17 * 101 // 100, 5 * 10**17 * 101 // 100)
    receipt_logs = [l for l in logs if l["address"] != NATIVE_TRACE]
    at_receipt = LpPlanner().amounts(shape, Ledger(receipt_logs, traced=False, native_flows={WALLET: -(6 * 10**17)}), 0, None)
    assert (at_receipt.amount0, at_receipt.amount1) == (6 * 10**17, 5 * 10**17)
    dec = LpShape(WALLET, LpOp.DECREASE, hookless, POSM, 77, -100, 100, 12345)
    dec_logs = [
        {"address": POOL_MANAGER, "topics": [tc.TOPIC_V4_MODIFY_LIQUIDITY, hookless.id, _topic_addr(POSM)], "data": "0x" + _word(-100) + _word(100) + _word(-12345) + _word(0)},
        transfer(NATIVE_TRACE, POOL_MANAGER, WALLET, 1_000),
        transfer(ITH, POOL_MANAGER, WALLET, 2_000),
    ]
    out = LpPlanner().amounts(dec, Ledger(dec_logs, traced=True), 100, 1 << 96)
    principal0, principal1 = out.amount0 - out.fees0, out.amount1 - out.fees1
    assert (out.amount0, out.amount1) == (1_000, 2_000) and 0 <= principal0 <= 1_000 and 0 <= principal1 <= 2_000
    assert (out.bound0, out.bound1) == (principal0 * 99 // 100, principal1 * 99 // 100)


def test_lp_quote_refuses_pons_adds_and_hook_reverts(core, rpc):
    with pytest.raises(TxRefusal) as refused:
        core.quote(LpIntent(wallet=WALLET, op=LpOp.MINT, pool_id=PONS.id, slippage_bps=50, tick_lower=0, tick_upper=200, amount0=1, amount1=1))
    assert refused.value.code == "pons_add"
    assert core.routes.pool(BLOCKED_ITH.id) == BLOCKED_ITH
    hookless = HOOKLESS_ITH
    wrapped = tc.selector("WrappedError(address,bytes4,bytes,bytes)") + encode(["address", "bytes4", "bytes", "bytes"], [NATIVE, b"\x25\x99\x82\xe5", b"", b""])
    rpc.simulate = lambda call, block: {"status": "0x0", "logs": [], "gasUsed": "0x10", "returnData": "0x" + wrapped.hex()}
    with pytest.raises(TxRefusal) as refused:
        core.quote(LpIntent(wallet=WALLET, op=LpOp.MINT, pool_id=hookless.id, slippage_bps=50, tick_lower=-100, tick_upper=100, amount0=10**18, amount1=10**18))
    assert refused.value.code == "hook_blocked_add"
    with pytest.raises(TxError) as err:
        core.quote(LpIntent(wallet=WALLET, op=LpOp.DECREASE, pool_id=hookless.id, slippage_bps=50, liquidity=1))
    assert err.value.code == "invalid_intent"
