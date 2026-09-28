"""End-to-end proofs on an anvil fork of Robinhood Chain (skipped without anvil or a fork RPC).

Every test drives TxCore from a fresh EOA funded with anvil_setBalance (the
anvil dev accounts carry EIP-7702 code on chain 4663 and Permit2 would route
their signatures through ERC-1271). Amounts are checked against balance deltas
to the wei.
"""
from __future__ import annotations

import atexit
import contextlib
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request
from dataclasses import replace

import pytest
from eth_utils import keccak

from rhpools import tx_chain as tc
from rhpools.tx_chain import (
    ADDRESS_THIS, CONTRACT_BALANCE, MSG_SENDER, NATIVE, NFPM_GIGA, NFPM_PANCAKE, NFPM_UNISWAP,
    PONS_HOOK, POOL_MANAGER, POSM, UR, UR_V2_FACTORY, UR_V3_FACTORY, USDG, WETH, PoolKey, V3Swap,
    WrapEth, erc20_balance_of, pool_manager_initialize, ur_execute, v3_path,
)
from rhpools.tx_core import Fill, LpQuote, SwapQuote, TxCore
from rhpools.tx_plan import Call, LpIntent, LpOp, Signatures, SwapIntent, TxError, TxPolicy, TxRefusal
from rhpools.tx_routes import RouteBook, Side

FOUNDRY = os.path.expanduser("~/.foundry/bin")
ANVIL = shutil.which("anvil") or os.path.join(FOUNDRY, "anvil")
CAST = shutil.which("cast") or os.path.join(FOUNDRY, "cast")
FORK_RPC = os.environ.get("RHP_FORK_RPC", "http://127.0.0.1:8547")
FEE_TO = "0x00000000000000000000000000000000000fee75"
ITH = "0xb3377f953994c6fa04383a64d88aa8122e81fd62"
PIPEDOG = "0x5cb6f181081301b44905f3ae15419112ecabd8a6"
ASTRO = "0x5b81efe5e0ba2b31c0ed843b908a2c45fe6e14c5"
PONS_POOL = "0xeda3dbcd4b745a70d04a92293bb7dcb3d9de234bd59e2773574a259cfe6f3fb0"
V4_ETH_USDG = PoolKey(NATIVE, USDG, 500, 10, NATIVE).id()
UNI_WETH_USDG = "0x52e65b17fb6e5ba00ed806f37afcd2daa50271ca"
PANCAKE_WETH_USDG = "0x88a8e96e7785d378825e8b5d7fc0e6f62487061e"
GIGA_WETH_USDG = "0xb2a6ad51b3ea3cdc8d3508cca147a43471382e53"
PANCAKE_FACTORY = "0x0bfbcf9fa4f9c56b0f40a671ad40e0805a091865"
GIGA_FACTORY = "0xece6ecd61177336ea6fb9b17937ac439d85ee20b"
PANCAKE_TOKEN = "0x9ac6678e9258822879baf0dc451f9b0fcdd74ba3"
GIGA_TOKEN = "0xf3081494b87e8d5fb7960f066e931d1d0e6e3d67"
PANCAKE_TOKEN_POOL = "0xd05e187dfa4740c30802f919f05b1b060ce2f6f2"
GIGA_TOKEN_POOL = "0x129b392490aac7b4320aeade8d1054c06c2ce224"
SLIPSTREAM_TOKEN = "0xd0601ce157db5bdc3162bbac2a2c8af5320d9eec"
SLIPSTREAM_TOKEN_POOL = "0x18a5af4e442f8be68968cc1f00d537f8af2d12cd"
SLIPSTREAM_FACTORY = "0x1ac9db4a2608ba45d6127b1737949b51bb54b7f3"
BLOCKING_HOOK = "0x00000000000000000000000000000000dead0800"
POOL_COLUMNS = "id, protocol, address, token0, token1, fee_ppm, tick_spacing, hook, factory"
POOL_ROWS = [
    (PONS_POOL, "v4", POOL_MANAGER, NATIVE, ITH, 0, 200, PONS_HOOK, POOL_MANAGER),
    ("0xb7f10f74b39291b9290b779978e19a7637c742d6", "v3", "0xb7f10f74b39291b9290b779978e19a7637c742d6", WETH, PIPEDOG, 10000, 200, None, UR_V3_FACTORY),
    ("0x3416b8d8aa6ae642ffdc2f65165ff479d4bd3007", "v2", "0x3416b8d8aa6ae642ffdc2f65165ff479d4bd3007", WETH, ASTRO, None, None, None, UR_V2_FACTORY),
    (UNI_WETH_USDG, "v3", UNI_WETH_USDG, WETH, USDG, 100, 1, None, UR_V3_FACTORY),
    (PANCAKE_WETH_USDG, "v3", PANCAKE_WETH_USDG, WETH, USDG, 500, 10, None, PANCAKE_FACTORY),
    (GIGA_WETH_USDG, "v3", GIGA_WETH_USDG, WETH, USDG, 100, 1, None, GIGA_FACTORY),
    (PANCAKE_TOKEN_POOL, "v3", PANCAKE_TOKEN_POOL, USDG, PANCAKE_TOKEN, 100, 1, None, PANCAKE_FACTORY),
    (GIGA_TOKEN_POOL, "v3", GIGA_TOKEN_POOL, USDG, GIGA_TOKEN, 3000, 60, None, GIGA_FACTORY),
    (SLIPSTREAM_TOKEN_POOL, "v3", SLIPSTREAM_TOKEN_POOL, USDG, SLIPSTREAM_TOKEN, 100, 60, None, SLIPSTREAM_FACTORY),
    (V4_ETH_USDG, "v4", POOL_MANAGER, NATIVE, USDG, 500, 10, NATIVE, POOL_MANAGER),
]


def _reachable(url: str) -> bool:
    try:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_chainId", "params": []}).encode()
        out = json.loads(urllib.request.urlopen(urllib.request.Request(url, body, {"content-type": "application/json"}), timeout=5).read())
        return int(out["result"], 16) == 4663
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not (os.path.exists(ANVIL) and os.path.exists(CAST) and _reachable(FORK_RPC)),
    reason="needs anvil, cast and a reachable chain-4663 fork RPC",
)


class Rpc:
    def __init__(self, url: str) -> None:
        self.url = url
        self._id = 0

    def call(self, method, params=None):
        self._id += 1
        body = json.dumps({"jsonrpc": "2.0", "id": self._id, "method": method, "params": list(params or [])}).encode()
        out = json.loads(urllib.request.urlopen(urllib.request.Request(self.url, body, {"content-type": "application/json"}), timeout=180).read())
        if "error" in out:
            raise RuntimeError(json.dumps(out["error"]))
        return out["result"]


class Fork:
    def __init__(self) -> None:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            self.port = s.getsockname()[1]
        self.process = subprocess.Popen(
            [ANVIL, "--fork-url", FORK_RPC, "--chain-id", "4663", "--port", str(self.port), "--silent"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        atexit.register(self.stop)
        self.rpc = Rpc(f"http://127.0.0.1:{self.port}")
        for _ in range(300):
            if _reachable(self.rpc.url):
                break
            time.sleep(0.1)
        else:
            self.stop()
            raise RuntimeError("anvil did not start")
        wallet = json.loads(subprocess.run([CAST, "wallet", "new", "--json"], check=True, capture_output=True, text=True).stdout)[0]
        self.user = wallet["address"].lower()
        self._key = wallet["private_key"]
        self.rpc.call("anvil_setBalance", [self.user, hex(10_000 * 10**18)])
        self.rpc.call("anvil_impersonateAccount", [self.user])
        assert self.rpc.call("eth_getCode", [self.user, "latest"]) in ("0x", "0x0")

    def stop(self) -> None:
        if self.process.poll() is None:
            self.process.kill()
            self.process.wait(timeout=10)

    def send(self, tx: dict) -> dict:
        request = {k: v for k, v in tx.items() if k != "chainId"}
        request.setdefault("gas", hex(3_000_000))
        tx_hash = self.rpc.call("eth_sendTransaction", [request])
        for _ in range(400):
            receipt = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
            if receipt is not None:
                return receipt
            time.sleep(0.05)
        raise RuntimeError("transaction not mined")

    def raw(self, to: str, data: bytes, value: int = 0) -> dict:
        receipt = self.send({"from": self.user, "to": to, "data": "0x" + data.hex(), "value": hex(value)})
        assert receipt["status"] == "0x1", receipt
        return receipt

    def call(self, to: str, data: bytes) -> bytes:
        return bytes.fromhex(self.rpc.call("eth_call", [{"from": self.user, "to": to, "data": "0x" + data.hex()}, "latest"])[2:])

    def balance(self, currency: str, who: str) -> int:
        if currency == NATIVE:
            return int(self.rpc.call("eth_getBalance", [who, "latest"]), 16)
        return int.from_bytes(self.call(currency, erc20_balance_of(who)), "big")

    def sign(self, typed_data: dict) -> bytes:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as fh:
            json.dump(typed_data, fh)
            path = fh.name
        try:
            out = subprocess.run([CAST, "wallet", "sign", "--private-key", self._key, "--data", "--from-file", path], check=True, capture_output=True, text=True).stdout.strip()
        finally:
            os.unlink(path)
        return bytes.fromhex(out[2:])

    def get_usdg(self, eth: int) -> None:
        data = ur_execute((WrapEth(ADDRESS_THIS, CONTRACT_BALANCE), V3Swap(MSG_SENDER, CONTRACT_BALANCE, 1, v3_path((WETH, USDG), (100,)), False)), 2_000_000_000)
        self.raw(UR, data, eth)

    def get_weth(self, eth: int) -> None:
        self.raw(WETH, tc.selector("deposit()"), eth)


@pytest.fixture(scope="module")
def fork():
    fork = Fork()
    try:
        yield fork
    finally:
        fork.stop()


@pytest.fixture(scope="module")
def pools_db(tmp_path_factory):
    path = tmp_path_factory.mktemp("market") / "pools.sqlite"
    connection = sqlite3.connect(path)
    connection.executescript(
        "CREATE TABLE pools(id TEXT PRIMARY KEY, protocol TEXT NOT NULL, address TEXT NOT NULL, token0 TEXT NOT NULL, token1 TEXT NOT NULL,"
        " fee_ppm INTEGER, tick_spacing INTEGER, hook TEXT, factory TEXT, metadata_json TEXT);"
        "CREATE TABLE lp_pool_state(pool_id TEXT PRIMARY KEY, block_number INTEGER NOT NULL);"
    )
    connection.executemany(f"INSERT INTO pools({POOL_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?)", POOL_ROWS)
    connection.commit()
    connection.close()
    return path


def _reader_for(path):
    @contextlib.contextmanager
    def reader():
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        try:
            yield connection
        finally:
            connection.close()

    return reader


@pytest.fixture(scope="module")
def core(fork, pools_db):
    routes = RouteBook(_reader_for(pools_db), fork.rpc)
    core = TxCore(fork.rpc, routes, TxPolicy(fee_bps=75, fee_recipient=FEE_TO))
    assert core.enabled, core.mismatches
    return core


def run_steps(core: TxCore, fork: Fork, quote) -> tuple[dict, Signatures]:
    sig = None
    for step in quote.steps:
        if step.kind == "approve":
            assert fork.send(step.tx)["status"] == "0x1"
        elif step.kind == "permit":
            sig = fork.sign(step.typed_data)
    sigs = Signatures(permit=sig)
    prepared = core.prepare(quote.quote_id, fork.user, sigs)
    assert prepared.transaction["to"] == quote.plan.to
    assert prepared.transaction["chainId"] == hex(4663)
    return prepared.transaction, sigs


def trade(core: TxCore, fork: Fork, intent: SwapIntent) -> tuple[SwapQuote, Fill]:
    quote = core.quote(intent)
    assert isinstance(quote, SwapQuote)
    tx, _ = run_steps(core, fork, quote)
    watched = {(c, w) for c in (intent.currency_in, intent.currency_out, quote.amounts.rhpools_fee.currency) for w in (fork.user, FEE_TO)}
    before = {k: fork.balance(*k) for k in watched}
    receipt = fork.send(tx)
    assert receipt["status"] == "0x1", receipt
    after = {k: fork.balance(*k) for k in watched}
    delta = {k: after[k] - before[k] for k in watched}
    fill = core.receipt(receipt["transactionHash"], fork.user)
    assert fill.status == "confirmed"
    amounts = fill.amounts
    gas = fill.gas_cost_native
    fee = quote.amounts.rhpools_fee
    assert delta[(fee.currency, FEE_TO)] == fee.amount == amounts.rhpools_fee.amount
    assert amounts.amount_in == quote.amounts.amount_in == intent.amount_in
    assert amounts.net_out == quote.amounts.net_out
    assert amounts.hook_fee == quote.amounts.hook_fee and amounts.creator_tax == quote.amounts.creator_tax
    assert quote.amounts.net_out >= quote.amounts.min_out
    user_gas = gas if NATIVE in (intent.currency_in, intent.currency_out) else 0
    if intent.currency_in == NATIVE:
        assert delta[(NATIVE, fork.user)] == -intent.amount_in - user_gas
    else:
        assert delta[(intent.currency_in, fork.user)] == -intent.amount_in
    if intent.currency_out == NATIVE:
        assert delta[(NATIVE, fork.user)] == amounts.net_out - user_gas
    else:
        assert delta[(intent.currency_out, fork.user)] == amounts.net_out
    if intent.side is Side.BUY and fee.currency == intent.currency_in:
        assert fee.amount == intent.amount_in * 75 // 10_000
    return quote, fill


def swap(wallet: str, side: Side, token: str, quote_currency: str, amount_in: int, slippage_bps: int = 100) -> SwapIntent:
    return SwapIntent(wallet=wallet, side=side, token=token, quote_currency=quote_currency, amount_in=amount_in, slippage_bps=slippage_bps)


def expect_steps(fork: Fork, quote, permit_tokens: dict[str, int]) -> None:
    approvals = sum(1 for t, amount in permit_tokens.items() if int.from_bytes(fork.call(t, tc.erc20_allowance(fork.user, tc.PERMIT2)), "big") < amount)
    permit = ["permit"] if permit_tokens else []
    assert [s.kind for s in quote.steps] == ["approve"] * approvals + permit + ["send"]



@pytest.mark.parametrize("token,router", [
    (PANCAKE_TOKEN, tc.PANCAKE_SMART_ROUTER),
    (GIGA_TOKEN, tc.GIGA_SWAP_ROUTER),
    (SLIPSTREAM_TOKEN, tc.SLIPSTREAM_SWAP_ROUTER),
])
def test_protocol_router_buy_sell(core, fork, token, router):
    fork.get_usdg(2 * 10**18)
    buy = swap(fork.user, Side.BUY, token, USDG, 100_000 if token == PANCAKE_TOKEN else 10 * 10**6)
    quote, _ = trade(core, fork, buy)
    assert quote.plan.to == router
    assert quote.amounts.rhpools_fee.currency == token
    assert quote.amounts.rhpools_fee.amount == quote.amounts.pool_out * 75 // 10_000
    held = fork.balance(token, fork.user)
    sell = swap(fork.user, Side.SELL, token, USDG, held // 2)
    quote, _ = trade(core, fork, sell)
    assert quote.plan.to == router
    assert quote.amounts.rhpools_fee.currency == USDG
    assert quote.amounts.rhpools_fee.amount == quote.amounts.pool_out * 75 // 10_000


@pytest.mark.parametrize("token,router,amount", [
    (PANCAKE_TOKEN, tc.PANCAKE_SMART_ROUTER, 10**13),
    (GIGA_TOKEN, tc.GIGA_SWAP_ROUTER, 10**14),
    (SLIPSTREAM_TOKEN, tc.SLIPSTREAM_SWAP_ROUTER, 10**14),
])
def test_protocol_router_bridged_buy_sell(core, fork, token, router, amount):
    fork.get_weth(amount * 3)
    buy = swap(fork.user, Side.BUY, token, WETH, amount)
    candidates = core.routes.candidates(token, WETH, Side.BUY)
    assert any(len(route.hops) == 2 for route in candidates)
    quote, _ = trade(core, fork, buy)
    assert quote.plan.to == router
    assert quote.amounts.rhpools_fee.currency == token
    held = fork.balance(token, fork.user)
    quote, _ = trade(core, fork, swap(fork.user, Side.SELL, token, WETH, held // 2))
    assert quote.plan.to == router
    assert quote.amounts.rhpools_fee.currency == WETH


def test_pons_buy_and_sell_from_usdg(core, fork):
    fork.get_usdg(2 * 10**18)
    intent = swap(fork.user, Side.BUY, ITH, USDG, 100 * 10**6)
    before_quote = core.quote(intent)
    expect_steps(fork, before_quote, {USDG: intent.amount_in})
    quote, fill = trade(core, fork, intent)
    assert [h.pool.venue.value for h in quote.route.hops] == ["v3", "v4"]
    assert quote.amounts.hook_fee.amount == quote.amounts.pool_out * 100 // 10_000
    assert quote.amounts.creator_tax.amount == quote.amounts.pool_out * 200 // 10_000
    assert quote.amounts.net_out == quote.amounts.pool_out - quote.amounts.hook_fee.amount - quote.amounts.creator_tax.amount
    assert "pons_fees" in quote.warnings
    assert quote.amounts.impact_bps is not None and quote.amounts.impact_bps >= 0
    held = fork.balance(ITH, fork.user)
    sell = swap(fork.user, Side.SELL, ITH, USDG, held // 2)
    expect_steps(fork, core.quote(sell), {ITH: sell.amount_in})
    quote, fill = trade(core, fork, sell)
    assert [h.pool.venue.value for h in quote.route.hops] == ["v4", "v3"]
    assert quote.amounts.rhpools_fee.currency == USDG


def test_pons_buy_and_sell_from_eth(core, fork):
    quote, fill = trade(core, fork, swap(fork.user, Side.BUY, ITH, NATIVE, 10**16))
    assert [h.pool.venue.value for h in quote.route.hops] == ["v4"]
    assert [s.kind for s in quote.steps] == ["send"]
    assert quote.amounts.rhpools_fee.currency == NATIVE
    held = fork.balance(ITH, fork.user)
    sell = swap(fork.user, Side.SELL, ITH, NATIVE, held // 3)
    expect_steps(fork, core.quote(sell), {ITH: sell.amount_in})
    quote, fill = trade(core, fork, sell)
    assert [h.pool.venue.value for h in quote.route.hops] == ["v4"]
    assert quote.amounts.rhpools_fee.currency == NATIVE
    assert quote.amounts.hook_fee.currency == NATIVE


def test_v3_token_buy_and_sell(core, fork):
    quote, _ = trade(core, fork, swap(fork.user, Side.BUY, PIPEDOG, NATIVE, 10**16))
    assert [h.pool.venue.value for h in quote.route.hops] == ["v3"]
    assert quote.amounts.hook_fee is None
    quote, _ = trade(core, fork, swap(fork.user, Side.BUY, PIPEDOG, USDG, 20 * 10**6))
    assert [h.pool.venue.value for h in quote.route.hops][-1] == "v3"
    held = fork.balance(PIPEDOG, fork.user)
    quote, _ = trade(core, fork, swap(fork.user, Side.SELL, PIPEDOG, NATIVE, held // 2))
    assert quote.amounts.rhpools_fee.currency == WETH
    quote, _ = trade(core, fork, swap(fork.user, Side.SELL, PIPEDOG, USDG, held // 4))
    assert quote.amounts.rhpools_fee.currency == USDG
    quote, _ = trade(core, fork, swap(fork.user, Side.SELL, PIPEDOG, WETH, held // 8))
    assert quote.amounts.rhpools_fee.currency == WETH


def test_v2_token_buy_and_sell(core, fork):
    quote, _ = trade(core, fork, swap(fork.user, Side.BUY, ASTRO, NATIVE, 10**16))
    assert [h.pool.venue.value for h in quote.route.hops] == ["v2"]
    quote, _ = trade(core, fork, swap(fork.user, Side.BUY, ASTRO, USDG, 10 * 10**6))
    assert [h.pool.venue.value for h in quote.route.hops][-1] == "v2"
    held = fork.balance(ASTRO, fork.user)
    trade(core, fork, swap(fork.user, Side.SELL, ASTRO, NATIVE, held // 2))
    trade(core, fork, swap(fork.user, Side.SELL, ASTRO, USDG, held // 4))


@pytest.mark.parametrize(
    "side,token,quote_currency,amount",
    [
        (Side.BUY, ITH, NATIVE, 10**16),
        (Side.SELL, ITH, NATIVE, 10**18),
        (Side.SELL, PIPEDOG, USDG, 10**12),
        (Side.SELL, ASTRO, NATIVE, 10**6),
        (Side.BUY, PIPEDOG, NATIVE, 10**15),
    ],
)
def test_min_out_plus_one_reverts(core, fork, side, token, quote_currency, amount):
    if side is Side.SELL:
        if fork.balance(token, fork.user) == 0:
            trade(core, fork, swap(fork.user, Side.BUY, token, NATIVE, 10**16))
        amount = fork.balance(token, fork.user) // 10
    quote = core.quote(swap(fork.user, side, token, quote_currency, amount, slippage_bps=0))
    sig = fork.sign(quote.step("permit").typed_data) if quote.plan.permit else None
    exact = core.swaps.finalize(quote.plan, quote.amounts.net_out, Signatures(permit=sig))
    assert core.sim.run(fork.user, (), Call(exact.to, exact.calldata(), exact.value), "latest").ok
    over = core.swaps.finalize(quote.plan, quote.amounts.net_out + 1, Signatures(permit=sig))
    result = core.sim.run(fork.user, (), Call(over.to, over.calldata(), over.value), "latest")
    assert not result.ok and result.revert.kind == "slippage", result.revert


def test_expired_deadline_reverts(core, fork):
    quote = core.quote(swap(fork.user, Side.BUY, ITH, NATIVE, 10**15))
    stale = replace(core.swaps.finalize(quote.plan, quote.amounts.min_out, Signatures()), deadline=1)
    result = core.sim.run(fork.user, (), Call(stale.to, stale.calldata(), stale.value), "latest")
    assert not result.ok and result.revert.kind == "expired" and result.revert.selector == "0x5bf6f916"


def test_prepare_is_idempotent_and_fails_after_landing(core, fork):
    held = fork.balance(ITH, fork.user)
    quote = core.quote(swap(fork.user, Side.SELL, ITH, NATIVE, held // 10))
    sigs = Signatures(permit=fork.sign(quote.step("permit").typed_data))
    first = core.prepare(quote.quote_id, fork.user, sigs)
    second = core.prepare(quote.quote_id, fork.user, sigs)
    assert first.transaction == second.transaction
    assert fork.send(first.transaction)["status"] == "0x1"
    with pytest.raises(TxError) as err:
        core.prepare(quote.quote_id, fork.user, sigs)
    assert err.value.code == "bad_signature"


def lp(wallet: str, op: LpOp, pool_id: str, **kw) -> LpIntent:
    return LpIntent(wallet=wallet, op=op, pool_id=pool_id, slippage_bps=kw.pop("slippage_bps", 50), **kw)


def lp_round_trip(core: TxCore, fork: Fork, intent: LpIntent) -> tuple[LpQuote, Fill]:
    quote = core.quote(intent)
    assert isinstance(quote, LpQuote)
    tx, _ = run_steps(core, fork, quote)
    c0, c1 = quote.pool.token0, quote.pool.token1
    before = (fork.balance(c0, fork.user), fork.balance(c1, fork.user))
    receipt = fork.send(tx)
    assert receipt["status"] == "0x1", receipt
    after = (fork.balance(c0, fork.user), fork.balance(c1, fork.user))
    fill = core.receipt(receipt["transactionHash"], fork.user)
    assert fill.status == "confirmed"
    sign = -1 if intent.op in (LpOp.MINT, LpOp.INCREASE) else 1
    gas0 = fill.gas_cost_native if c0 == NATIVE else 0
    assert after[0] - before[0] == sign * quote.amounts.amount0 - gas0
    assert after[1] - before[1] == sign * quote.amounts.amount1
    assert (fill.amounts.amount0, fill.amounts.amount1, fill.amounts.liquidity) == (quote.amounts.amount0, quote.amounts.amount1, quote.amounts.liquidity)
    return quote, fill


def lp_bound_probe(core: TxCore, fork: Fork, quote: LpQuote, bounds: tuple[int, int], sig: bytes | None):
    plan = core.lps.finalize(quote.plan, bounds, Signatures(permit=sig))
    return core.sim.run(fork.user, quote.plan.staging, Call(plan.to, plan.calldata(), plan.value), "latest")


def v3_tick(fork: Fork, pool: str) -> int:
    raw = fork.call(pool, tc.selector("slot0()"))
    tick = int.from_bytes(raw[32:64], "big")
    return tick - (1 << 256) if tick >= 1 << 255 else tick


@pytest.mark.parametrize(
    "manager,pool_id,fee,spacing",
    [(NFPM_UNISWAP, UNI_WETH_USDG, 100, 1), (NFPM_PANCAKE, PANCAKE_WETH_USDG, 500, 10), (NFPM_GIGA, GIGA_WETH_USDG, 100, 1)],
)
def test_v3_nfpm_lifecycle(core, fork, manager, pool_id, fee, spacing):
    fork.get_weth(10**17)
    fork.get_usdg(10**18)
    tick = v3_tick(fork, pool_id)
    lower = (tick // spacing - 20) * spacing
    upper = (tick // spacing + 20) * spacing
    weth, usdg = 10**16, 30 * 10**6
    mint = lp(fork.user, LpOp.MINT, pool_id, tick_lower=lower, tick_upper=upper, amount0=weth, amount1=usdg)
    quote, fill = lp_round_trip(core, fork, mint)
    assert quote.plan.to == manager and [s.kind for s in quote.steps] == ["approve", "approve", "send"]
    assert fill.amounts.token_id == quote.amounts.token_id and quote.amounts.token_id is not None
    assert 0 < quote.amounts.amount0 <= weth and 0 < quote.amounts.amount1 <= usdg
    token_id = quote.amounts.token_id
    view = core.pool_view(pool_id, fork.user)
    assert {p["token_id"]: int(p["liquidity"]) for p in view["positions"]}[str(token_id)] == quote.amounts.liquidity
    assert view["manager"] == manager and view["tick_spacing"] == spacing
    probe = core.quote(mint)
    assert lp_bound_probe(core, fork, probe, (probe.amounts.amount0 + 1, probe.amounts.amount1), None).revert.kind == "slippage"
    inc = lp(fork.user, LpOp.INCREASE, pool_id, token_id=token_id, amount0=weth // 2, amount1=usdg // 2)
    quote, _ = lp_round_trip(core, fork, inc)
    assert quote.amounts.liquidity > 0
    probe = core.quote(inc)
    assert lp_bound_probe(core, fork, probe, (probe.amounts.amount0, probe.amounts.amount1 + 1), None).revert.kind == "slippage"
    fork.get_usdg(5 * 10**17)
    liquidity = int.from_bytes(fork.call(manager, tc.nfpm_positions(token_id))[7 * 32:8 * 32], "big")
    col = lp(fork.user, LpOp.COLLECT, pool_id, token_id=token_id)
    quote, _ = lp_round_trip(core, fork, col)
    assert quote.amounts.liquidity == 0 and (quote.amounts.fees0, quote.amounts.fees1) == (quote.amounts.amount0, quote.amounts.amount1)
    dec = lp(fork.user, LpOp.DECREASE, pool_id, token_id=token_id, liquidity=liquidity)
    probe = core.quote(dec)
    principal0 = probe.amounts.amount0 - probe.amounts.fees0
    principal1 = probe.amounts.amount1 - probe.amounts.fees1
    assert lp_bound_probe(core, fork, probe, (principal0, principal1), None).ok
    assert lp_bound_probe(core, fork, probe, (principal0 + 1, principal1), None).revert.kind == "slippage"
    quote, fill = lp_round_trip(core, fork, dec)
    assert quote.amounts.liquidity == liquidity and quote.amounts.amount0 > 0 and quote.amounts.amount1 > 0
    assert (fill.amounts.fees0, fill.amounts.fees1) == (quote.amounts.fees0, quote.amounts.fees1)


def v4_tick(fork: Fork, pool_id: str) -> int:
    raw = fork.call(tc.STATE_VIEW, tc.state_view_slot0(pool_id))
    tick = int.from_bytes(raw[32:64], "big") & 0xFFFFFF
    return tick - (1 << 24) if tick >= 1 << 23 else tick


def test_v4_posm_lifecycle_with_permit_batch(core, fork):
    fork.get_usdg(2 * 10**18)
    tick = v4_tick(fork, V4_ETH_USDG)
    lower, upper = (tick // 10 - 30) * 10, (tick // 10 + 30) * 10
    eth, usdg = 10**16, 50 * 10**6
    mint = lp(fork.user, LpOp.MINT, V4_ETH_USDG, tick_lower=lower, tick_upper=upper, amount0=eth, amount1=usdg)
    expect_steps(fork, core.quote(mint), {USDG: usdg})
    quote, fill = lp_round_trip(core, fork, mint)
    assert quote.plan.to == POSM
    assert quote.step("permit").typed_data["primaryType"] == "PermitBatch"
    assert 0 < quote.amounts.amount0 <= eth and 0 < quote.amounts.amount1 <= usdg
    token_id = quote.amounts.token_id
    assert token_id is not None and fill.amounts.token_id == token_id
    assert not [p for p in core.pool_view(V4_ETH_USDG, fork.user)["positions"] if p["token_id"] == str(token_id)]
    assert [p["token_id"] for p in core.pool_view(V4_ETH_USDG, fork.user, (token_id, 1))["positions"]] == [str(token_id)]
    probe = core.quote(mint)
    sig = fork.sign(probe.step("permit").typed_data)
    assert lp_bound_probe(core, fork, probe, (probe.amounts.amount0 - 1, probe.amounts.amount1), sig).revert.kind == "slippage"
    inc = lp(fork.user, LpOp.INCREASE, V4_ETH_USDG, token_id=token_id, amount0=eth // 2, amount1=usdg // 2)
    quote, _ = lp_round_trip(core, fork, inc)
    assert quote.amounts.liquidity > 0 and [s.kind for s in quote.steps] == ["permit", "send"]
    probe = core.quote(inc)
    sig = fork.sign(probe.step("permit").typed_data)
    assert lp_bound_probe(core, fork, probe, (probe.amounts.amount0, probe.amounts.amount1 - 1), sig).revert.kind == "slippage"
    liquidity = int.from_bytes(fork.call(POSM, tc.posm_position_liquidity(token_id)), "big")
    col = lp(fork.user, LpOp.COLLECT, V4_ETH_USDG, token_id=token_id)
    quote, _ = lp_round_trip(core, fork, col)
    assert quote.amounts.liquidity == 0 and [s.kind for s in quote.steps] == ["send"]
    dec = lp(fork.user, LpOp.DECREASE, V4_ETH_USDG, token_id=token_id, liquidity=liquidity)
    probe = core.quote(dec)
    principal = (probe.amounts.amount0 - probe.amounts.fees0, probe.amounts.amount1 - probe.amounts.fees1)
    assert lp_bound_probe(core, fork, probe, principal, None).ok
    assert lp_bound_probe(core, fork, probe, (principal[0] + 1, principal[1]), None).revert.kind == "slippage"
    assert lp_bound_probe(core, fork, probe, (principal[0], principal[1] + 1), None).revert.kind == "slippage"
    quote, fill = lp_round_trip(core, fork, dec)
    assert quote.amounts.liquidity == liquidity and quote.amounts.amount0 > 0 and quote.amounts.amount1 > 0
    assert int.from_bytes(fork.call(POSM, tc.posm_position_liquidity(token_id)), "big") == 0


def test_pons_add_refused_but_remove_allowed(core, fork):
    with pytest.raises(TxRefusal) as refused:
        core.quote(lp(fork.user, LpOp.MINT, PONS_POOL, tick_lower=199400, tick_upper=203400, amount0=10**16, amount1=10**18))
    assert refused.value.code == "pons_add"
    with pytest.raises(TxError) as err:
        core.quote(lp(fork.user, LpOp.DECREASE, PONS_POOL, token_id=1, liquidity=1))
    assert err.value.code == "not_owner"


def _blocking_hook_code() -> bytes:
    before_add = keccak(text="beforeAddLiquidity(address,(address,address,uint24,int24,address),(int24,int24,int256,bytes32),bytes)")[:4]
    assert before_add.hex() == "259982e5"
    return (
        bytes.fromhex("6000358060e01c63") + before_add + bytes.fromhex("14601857")
        + bytes.fromhex("60005260206000f3")
        + bytes.fromhex("5b600080fd")
    )


def test_hook_blocked_add_from_synthetic_before_add_hook(core, fork, pools_db):
    fork.rpc.call("anvil_setCode", [BLOCKING_HOOK, "0x" + _blocking_hook_code().hex()])
    assert tc.hook_flags(BLOCKING_HOOK) == {"BEFORE_ADD_LIQUIDITY"}
    key = PoolKey(NATIVE, ITH, 3000, 60, BLOCKING_HOOK)
    sqrt_price = int.from_bytes(fork.call(tc.STATE_VIEW, tc.state_view_slot0(PONS_POOL))[:32], "big")
    fork.raw(POOL_MANAGER, pool_manager_initialize(key, sqrt_price))
    connection = sqlite3.connect(pools_db)
    connection.execute(f"INSERT INTO pools({POOL_COLUMNS}) VALUES (?,?,?,?,?,?,?,?,?)", (key.id(), "v4", POOL_MANAGER, NATIVE, ITH, 3000, 60, BLOCKING_HOOK, POOL_MANAGER))
    connection.commit()
    connection.close()
    if fork.balance(ITH, fork.user) < 10**18:
        trade(core, fork, swap(fork.user, Side.BUY, ITH, NATIVE, 10**16))
    tick = v4_tick(fork, key.id())
    intent = lp(fork.user, LpOp.MINT, key.id(), tick_lower=(tick // 60 - 10) * 60, tick_upper=(tick // 60 + 10) * 60, amount0=10**15, amount1=10**18)
    with pytest.raises(TxRefusal) as refused:
        core.quote(intent)
    assert refused.value.code == "hook_blocked_add"
    assert BLOCKING_HOOK in refused.value.detail


def test_allowlist_mismatch_disables_trade(fork, pools_db):
    original = fork.rpc.call("eth_getCode", [UR, "latest"])
    fork.rpc.call("anvil_setCode", [UR, "0x6000"])
    try:
        routes = RouteBook(_reader_for(pools_db), fork.rpc)
        core = TxCore(fork.rpc, routes, TxPolicy(fee_bps=75, fee_recipient=FEE_TO))
        assert not core.enabled and [m.address for m in core.mismatches] == [UR]
        with pytest.raises(TxRefusal) as refused:
            core.quote(swap(fork.user, Side.BUY, ITH, NATIVE, 10**15))
        assert refused.value.code == "allowlist_mismatch"
    finally:
        fork.rpc.call("anvil_setCode", [UR, original])
    core = TxCore(fork.rpc, routes, TxPolicy(fee_bps=75, fee_recipient=FEE_TO))
    quote = core.quote(swap(fork.user, Side.BUY, ITH, NATIVE, 10**15))
    fork.rpc.call("anvil_setCode", [UR, "0x6000"])
    try:
        with pytest.raises(TxError) as err:
            core.prepare(quote.quote_id, fork.user, Signatures())
        assert err.value.code == "allowlist_mismatch"
        assert not core.enabled
    finally:
        fork.rpc.call("anvil_setCode", [UR, original])
