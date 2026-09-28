"""The shared transaction core. Never signs, never sends; the wallet signs what it is handed."""
from __future__ import annotations

import hashlib
import json
import os
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any
from urllib.request import Request, urlopen

from eth_abi import decode
from . import tx_allowlist
from .tx_chain import (
    ADD_GOVERNING_FLAGS, CHAIN_ID, DEX_BY_FACTORY, GIGA_SWAP_ROUTER, GIGA_V3_FACTORY, MSG_SENDER, NATIVE,
    NFPM_BY_FACTORY, NFPM_GIGA, NFPM_PANCAKE, NFPM_UNISWAP, PANCAKE_SMART_ROUTER,
    PANCAKE_V3_FACTORY, PERMIT2, PONS_HOOK, POSM, SEL_PANCAKE_EXACT_INPUT,
    SEL_PANCAKE_EXACT_INPUT_SINGLE, SEL_ROUTER_MULTICALL_DEADLINE, SEL_SLIPSTREAM_EXACT_INPUT_SINGLE,
    SEL_SWEEP_WITH_FEE, SEL_UNWRAP_WITH_FEE, SEL_V3_EXACT_INPUT, SEL_V3_EXACT_INPUT_SINGLE,
    SLIPSTREAM_FACTORY, SLIPSTREAM_SWAP_ROUTER, STATE_VIEW, UR, WETH,
    NfpmCollect, NfpmDecrease, NfpmIncrease, NfpmMint, PayPortion,
    Permit2TransferFrom, PoolKey, PosmDecrease, PosmIncrease, PosmMint, RevertKind, SEL_MULTICALL,
    SEL_POSM_PERMIT_BATCH, Sweep, UnwrapWeth, V2Swap, V3Swap, V4Swap, V4SwapExactInSingle,
    decode_multicall, decode_nfpm_call, decode_posm_modify_liquidities, decode_revert,
    decode_ur_execute, decode_v3_path, erc20_allowance, erc20_approve, erc20_balance_of,
    hook_flags, nfpm_positions, permit2_allowance, posm_pool_and_position,
    posm_position_liquidity, selector, state_view_slot0,
)
from .tx_plan import (
    Amounts, Call, FeeLeg, Ledger, LpAmounts, LpIntent, LpOp, LpPlanner, LpShape, Plan,
    PositionState, Signatures, SwapIntent, SwapPlanner, SwapShape, TxError, TxPolicy,
    TxRefusal, impact_bps, min_out_for, swap_amounts,
)
from .tx_routes import QUOTE_CURRENCIES, Hop, IncompletePool, Pool, Route, RouteBook, Side, Venue, v4_pool

DEADLINE_GRACE_S = 60
IMPACT_DIVISOR = 100
GAS_HEADROOM_PCT = 130
_ADDRESS_RE = re.compile(r"^0x[0-9a-f]{40}$")
_HASH_RE = re.compile(r"^0x[0-9a-f]{64}$")
SEL_OWNER_OF = selector("ownerOf(uint256)")
SEL_TOKEN_OF_OWNER_BY_INDEX = selector("tokenOfOwnerByIndex(address,uint256)")
NFPM_FACTORY = {manager: factory for factory, manager in NFPM_BY_FACTORY.items()}
LP_TARGETS = frozenset({POSM, NFPM_UNISWAP, NFPM_PANCAKE, NFPM_GIGA})

SEL_DECIMALS = selector("decimals()")
SEL_SYMBOL = selector("symbol()")


class JsonRpc:
    """Plain JSON-RPC over HTTP; errors carry the node's error object text."""

    def __init__(self, url: str, *, timeout: float = 10.0) -> None:
        self.url = url
        self.timeout = timeout

    def call(self, method: str, params: list[Any] | None = None) -> Any:
        body = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": list(params or [])}).encode()
        with urlopen(Request(self.url, body, {"Content-Type": "application/json"}), timeout=self.timeout) as response:
            reply = json.loads(response.read())
        if not isinstance(reply, dict) or "result" not in reply:
            raise RuntimeError(json.dumps((reply or {}).get("error") if isinstance(reply, dict) else reply))
        return reply["result"]

    def batch(self, calls: Any, size: int = 200) -> list[Any]:
        calls = list(calls)
        results: list[Any] = []
        for start in range(0, len(calls), size):
            chunk = calls[start:start + size]
            payload = [{"jsonrpc": "2.0", "id": i, "method": m, "params": list(p)} for i, (m, p) in enumerate(chunk)]
            with urlopen(Request(self.url, json.dumps(payload).encode(), {"Content-Type": "application/json"}), timeout=self.timeout) as response:
                replies = json.loads(response.read())
            if not isinstance(replies, list):
                raise RuntimeError(json.dumps(replies))
            by_id = {item.get("id"): item for item in replies}
            for index in range(len(chunk)):
                item = by_id.get(index) or {}
                if "result" not in item:
                    raise RuntimeError(json.dumps(item.get("error")))
                results.append(item["result"])
        return results


@dataclass(frozen=True)
class SimResult:
    ok: bool
    logs: list[dict[str, Any]]
    gas_used: int
    revert: RevertKind | None


class Simulator:
    def __init__(self, rpc: Any) -> None:
        self._rpc = rpc

    def run(self, wallet: str, staging: tuple[Call, ...], call: Call, block: str) -> SimResult:
        calls = [
            {"from": wallet, "to": c.to, "data": "0x" + c.data.hex(), "value": hex(c.value)}
            for c in (*staging, call)
        ]
        params = [{"blockStateCalls": [{"calls": calls}], "traceTransfers": True, "validation": False}, block]
        try:
            result = self._rpc.call("eth_simulateV1", params)
        except Exception as exc:
            text = " ".join(str(exc).split())
            if "insufficient funds" in text.lower():
                raise TxRefusal("insufficient_balance", text[:200]) from exc
            raise TxError("rpc", text[:200]) from exc
        outs = result[0]["calls"]
        for index, out in enumerate(outs[:-1]):
            if out.get("status") != "0x1":
                raise TxError("staging_failed", f"staging call {index} reverted: {_revert_bytes(out).hex()[:80]}")
        last = outs[-1]
        gas = int(last.get("gasUsed", "0x0"), 16)
        if last.get("status") == "0x1":
            return SimResult(True, list(last.get("logs", [])), gas, None)
        return SimResult(False, [], gas, decode_revert(_revert_bytes(last)))


def _revert_bytes(out: dict[str, Any]) -> bytes:
    error = out.get("error") or {}
    data = error.get("data") if isinstance(error, dict) else None
    if not isinstance(data, str) or not data.startswith("0x"):
        data = out.get("returnData", "0x")
    return bytes.fromhex(data[2:]) if isinstance(data, str) and data.startswith("0x") else b""


@dataclass(frozen=True)
class Block:
    number: int
    hash: str

    def to_json(self) -> dict[str, Any]:
        return {"number": self.number, "hash": self.hash}


@dataclass(frozen=True)
class Step:
    kind: str
    tx: dict[str, Any] | None = None
    typed_data: dict[str, Any] | None = None

    def to_json(self) -> dict[str, Any]:
        out: dict[str, Any] = {"kind": self.kind}
        if self.tx is not None:
            out["tx"] = self.tx
        if self.typed_data is not None:
            out["typed_data"] = self.typed_data
        return out


@dataclass(frozen=True)
class QuoteBase:
    quote_id: str
    wallet: str
    plan: Plan
    block: Block
    expires_at: int
    deadline: int
    steps: tuple[Step, ...]
    warnings: tuple[str, ...]
    gas: int

    def step(self, kind: str) -> Step:
        for step in self.steps:
            if step.kind == kind:
                return step
        raise KeyError(kind)

    def _base_json(self) -> dict[str, Any]:
        return {
            "quote_id": self.quote_id,
            "wallet": self.wallet,
            "block": self.block.to_json(),
            "expires_at": self.expires_at,
            "deadline": self.deadline,
            "steps": [s.to_json() for s in self.steps],
            "warnings": list(self.warnings),
            "gas": self.gas,
        }


@dataclass(frozen=True)
class SwapQuote(QuoteBase):
    intent: SwapIntent
    route: Route
    amounts: Amounts
    hop_policies: tuple[tuple[int, int], ...] = ()

    def to_json(self) -> dict[str, Any]:
        policies = self.hop_policies or tuple((0, 0) for _ in self.route.hops)
        return {
            **self._base_json(),
            "kind": "swap",
            "intent": self.intent.to_json(),
            "route": self.route.describe(),
            "hops": [
                {
                    "venue": hop.pool.venue.value, "dex": DEX_BY_FACTORY.get(hop.pool.factory or "", "uniswap"),
                    "pool_id": hop.pool.id, "fee_ppm": hop.pool.fee_ppm,
                    "currency_in": hop.currency_in, "currency_out": hop.currency_out,
                    "hook_fee_bps": hook_bps, "creator_tax_bps": creator_bps,
                }
                for hop, (hook_bps, creator_bps) in zip(self.route.hops, policies)
            ],
            "amounts": self.amounts.to_json(),
        }


@dataclass(frozen=True)
class LpQuote(QuoteBase):
    intent: LpIntent
    pool: Pool
    amounts: LpAmounts

    def to_json(self) -> dict[str, Any]:
        return {
            **self._base_json(),
            "kind": "lp",
            "intent": self.intent.to_json(),
            "pool_id": self.pool.id,
            "manager": self.plan.to,
            "amounts": self.amounts.to_json(),
        }


Quote = SwapQuote | LpQuote


@dataclass(frozen=True)
class Prepared:
    transaction: dict[str, Any]
    gas_used: int
    expires_at: int

    def to_json(self) -> dict[str, Any]:
        return {"transaction": dict(self.transaction), "simulation": {"gas_used": self.gas_used}, "expires_at": self.expires_at}


@dataclass(frozen=True)
class Fill:
    hash: str
    status: str
    block: int | None
    amounts: Amounts | LpAmounts | None
    gas_used: int | None
    gas_cost_native: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "hash": self.hash,
            "status": self.status,
            "block": self.block,
            "amounts": self.amounts.to_json() if self.amounts is not None else None,
            "gas_used": self.gas_used,
            "gas_cost_native": None if self.gas_cost_native is None else str(self.gas_cost_native),
        }


def _address(value: Any, field: str) -> str:
    if not isinstance(value, str) or _ADDRESS_RE.fullmatch(value.lower()) is None:
        raise TxError("invalid_intent", f"{field} must be a 20-byte hex address")
    return value.lower()


def _hex_int(value: Any, field: str) -> int:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise TxError("rpc", f"malformed {field}")
    return int(value, 16)


def _word(value: Any, index: int = 0) -> int:
    if not isinstance(value, str) or len(value) < 2 + 64 * (index + 1):
        raise TxError("rpc", "short eth_call result")
    return int(value[2 + 64 * index:2 + 64 * (index + 1)], 16)


def _abi_string(value: Any) -> str:
    """ABI string or bytes32 symbol; unreadable symbols become an empty string."""
    raw = bytes.fromhex(value[2:]) if isinstance(value, str) and value.startswith("0x") else b""
    if len(raw) >= 64:
        length = int.from_bytes(raw[32:64], "big")
        if 64 + length <= len(raw):
            return raw[64:64 + length].decode("utf-8", "replace")[:24]
    return raw[:32].rstrip(b"\0").decode("utf-8", "replace")[:24]


def _signed24(value: int) -> int:
    value &= 0xFFFFFF
    return value - (1 << 24) if value >= 1 << 23 else value


class TxCore:
    def __init__(
        self,
        rpc: Any,
        routes: RouteBook,
        policy: TxPolicy,
        *,
        ttl_s: int = 60,
        max_quotes: int = 512,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if not hasattr(rpc, "call"):
            raise TypeError("rpc must provide call(method, params)")
        self.rpc = rpc
        self.routes = routes
        self.policy = policy
        self.ttl_s = ttl_s
        self.max_quotes = max_quotes
        self._clock = clock
        self.swaps = SwapPlanner()
        self.lps = LpPlanner()
        self.sim = Simulator(rpc)
        self._quotes: dict[str, Quote] = {}
        self._token_meta: dict[str, tuple[int, str]] = {}
        self._lock = threading.Lock()
        self.mismatches = tx_allowlist.verify(rpc)

    @property
    def enabled(self) -> bool:
        return not self.mismatches

    def close(self) -> None:
        with self._lock:
            self._quotes.clear()

    def _require_enabled(self) -> None:
        if self.mismatches:
            bad = ", ".join(m.address for m in self.mismatches)
            raise TxRefusal("allowlist_mismatch", f"target code changed: {bad}")

    def _call(self, to: str, data: bytes, block: str) -> str:
        return self.rpc.call("eth_call", [{"to": to, "data": "0x" + data.hex()}, block])

    def _header(self, block: str) -> Block:
        header = self.rpc.call("eth_getBlockByNumber", [block, False])
        if not isinstance(header, dict):
            raise TxError("rpc", "no block header")
        number = _hex_int(header.get("number"), "block number")
        block_hash = header.get("hash")
        if not isinstance(block_hash, str) or _HASH_RE.fullmatch(block_hash.lower()) is None:
            raise TxError("rpc", "malformed block hash")
        return Block(number, block_hash.lower())

    def _balance(self, currency: str, wallet: str, tag: str) -> int:
        if currency == NATIVE:
            return _hex_int(self.rpc.call("eth_getBalance", [wallet, tag]), "balance")
        return _word(self._call(currency, erc20_balance_of(wallet), tag))

    def _allowance(self, token: str, owner: str, spender: str, tag: str) -> int:
        return _word(self._call(token, erc20_allowance(owner, spender), tag))

    def balances(self, wallet: str, currencies: list[str]) -> dict[str, dict[str, Any]]:
        """Wallet balance, decimals and symbol per currency at latest; NATIVE is ETH."""
        if _ADDRESS_RE.fullmatch(wallet.lower()) is None:
            raise TxError("invalid_intent", "wallet must be an address")
        out: dict[str, dict[str, Any]] = {}
        for currency in dict.fromkeys(c.lower() for c in currencies[:8]):
            if _ADDRESS_RE.fullmatch(currency) is None:
                raise TxError("invalid_intent", "currency must be an address")
            if currency == NATIVE:
                out[currency] = {"balance": str(self._balance(NATIVE, wallet, "latest")), "decimals": 18, "symbol": "ETH"}
                continue
            with self._lock:
                meta = self._token_meta.get(currency)
            if meta is None:
                decimals = _word(self._call(currency, SEL_DECIMALS, "latest"))
                meta = (decimals, _abi_string(self._call(currency, SEL_SYMBOL, "latest")))
                with self._lock:
                    self._token_meta[currency] = meta
            out[currency] = {"balance": str(self._balance(currency, wallet, "latest")), "decimals": meta[0], "symbol": meta[1]}
        return out

    def _permit_nonce(self, wallet: str, token: str, spender: str, tag: str) -> int:
        return _word(self._call(PERMIT2, permit2_allowance(wallet, token, spender), tag), 2)

    def _tx(self, wallet: str, to: str, data: bytes, value: int = 0, gas: int | None = None) -> dict[str, Any]:
        tx = {"from": wallet, "to": to, "data": "0x" + data.hex(), "value": hex(value), "chainId": hex(CHAIN_ID)}
        if gas is not None:
            tx["gas"] = hex(gas)
        return tx

    def _steps(self, wallet: str, plan: Plan, tag: str) -> tuple[Step, ...]:
        steps: list[Step] = []
        for need in plan.approvals:
            if self._allowance(need.token, wallet, need.spender, tag) < need.required:
                steps.append(Step("approve", tx=self._tx(wallet, need.token, erc20_approve(need.spender, need.approve_amount))))
        typed = plan.permit_typed_data()
        if typed is not None:
            steps.append(Step("permit", typed_data=typed))
        steps.append(Step("send"))
        return tuple(steps)

    def _store(self, quote: Quote) -> None:
        now = int(self._clock())
        with self._lock:
            for key in [k for k, q in self._quotes.items() if q.expires_at <= now]:
                self._quotes.pop(key, None)
            while len(self._quotes) >= self.max_quotes:
                self._quotes.pop(next(iter(self._quotes)))
            self._quotes[quote.quote_id] = quote

    def _quote_id(self, intent: SwapIntent | LpIntent, block: Block) -> str:
        canonical = repr((intent, block)).encode()
        return hashlib.sha256(os.urandom(32) + canonical).hexdigest()

    def _times(self) -> tuple[int, int]:
        expires_at = int(self._clock()) + self.ttl_s
        return expires_at, expires_at + DEADLINE_GRACE_S

    def quote(self, intent: SwapIntent | LpIntent) -> Quote:
        self._require_enabled()
        if isinstance(intent, SwapIntent):
            return self._quote_swap(intent)
        if isinstance(intent, LpIntent):
            return self._quote_lp(intent)
        raise TxError("invalid_intent", "unknown intent")

    def _validate_swap(self, intent: SwapIntent) -> SwapIntent:
        intent = replace(
            intent,
            wallet=_address(intent.wallet, "wallet"),
            token=_address(intent.token, "token"),
            quote_currency=_address(intent.quote_currency, "quote_currency"),
        )
        if intent.quote_currency not in QUOTE_CURRENCIES:
            raise TxError("invalid_intent", "quote_currency must be ETH, WETH or USDG")
        if intent.token in QUOTE_CURRENCIES:
            raise TxError("invalid_intent", "token must not be a quote currency")
        if not isinstance(intent.amount_in, int) or isinstance(intent.amount_in, bool) or not 0 < intent.amount_in < 1 << 160:
            raise TxError("invalid_intent", "amount_in must be a positive integer below 2**160")
        if not isinstance(intent.slippage_bps, int) or not 0 <= intent.slippage_bps <= 5_000:
            raise TxError("invalid_intent", "slippage_bps must be within 0..5000")
        if not isinstance(intent.side, Side):
            raise TxError("invalid_intent", "side must be buy or sell")
        return intent

    def _simulate_swap(self, intent: SwapIntent, route: Route, deadline: int, nonce: int, tag: str) -> tuple[Plan, SimResult, Amounts | None, TxError | None]:
        plan = self.swaps.plan(intent, route, self.policy, deadline, nonce)
        sim = self.sim.run(intent.wallet, plan.staging, Call(plan.to, plan.calldata(), plan.value), tag)
        if not sim.ok:
            assert sim.revert is not None
            return plan, sim, None, TxError(sim.revert.kind, f"{sim.revert.selector} {sim.revert.detail}".strip())
        try:
            amounts = swap_amounts(plan.shape, Ledger(sim.logs, traced=True), self.routes.hook_policy)
        except TxError as exc:
            return plan, sim, None, exc
        return plan, sim, amounts, None

    def _quote_swap(self, intent: SwapIntent) -> SwapQuote:
        intent = self._validate_swap(intent)
        block = self._header("latest")
        tag = hex(block.number)
        if self._balance(intent.currency_in, intent.wallet, tag) < intent.amount_in:
            raise TxRefusal("insufficient_balance", "wallet holds less than amount_in")
        candidates = self.routes.candidates(intent.token, intent.quote_currency, intent.side)
        if not candidates:
            factories = sorted({pool.factory for pool in self.routes.token_pools(intent.token) if not pool.swappable})
            detail = "unsupported pool factories: " + ", ".join(factories) if factories else "no compatible route with a deployed router"
            raise TxRefusal("no_route", detail)
        expires_at, deadline = self._times()
        nonce = 0 if intent.currency_in == NATIVE else self._permit_nonce(intent.wallet, intent.currency_in, UR, tag)
        best: tuple[Plan, SimResult, Amounts, Route] | None = None
        failures: list[tuple[Route, TxError]] = []
        for route in candidates:
            plan, sim, amounts, failure = self._simulate_swap(intent, route, deadline, nonce, tag)
            if amounts is None:
                assert failure is not None
                failures.append((route, failure))
                continue
            if best is None or amounts.net_out > best[2].net_out:
                best = (plan, sim, amounts, route)
        if best is None:
            codes = {f.code for _, f in failures}
            code = next(iter(codes)) if codes <= {"insufficient_balance", "unmodeled_fee"} and len(codes) == 1 else "no_route"
            raise TxRefusal(code, "; ".join(f"{r.describe()}: {f}" for r, f in failures)[:400])
        plan, sim, amounts, route = best
        warnings: list[str] = []
        small_intent = replace(intent, amount_in=max(1, intent.amount_in // IMPACT_DIVISOR))
        _, _, small, _ = self._simulate_swap(small_intent, route, deadline, nonce, tag)
        impact = impact_bps(amounts, small) if small is not None else None
        if impact is None:
            warnings.append("impact_unavailable")
        elif impact > self.policy.max_impact_bps:
            raise TxRefusal("impact_over_limit", f"impact {impact} bps exceeds {self.policy.max_impact_bps}")
        amounts = replace(amounts, min_out=min_out_for(amounts.net_out, intent.slippage_bps), impact_bps=impact)
        if amounts.hook_fee is not None:
            warnings.append("pons_fees")
        steps = self._steps(intent.wallet, plan, tag)
        quote = SwapQuote(
            quote_id=self._quote_id(intent, block),
            wallet=intent.wallet,
            plan=plan,
            block=block,
            expires_at=expires_at,
            deadline=deadline,
            steps=steps,
            warnings=tuple(warnings),
            gas=sim.gas_used,
            intent=intent,
            route=route,
            amounts=amounts,
            hop_policies=tuple(
                (policy.hook_fee_bps, policy.creator_tax_bps)
                for policy in (self.routes.hook_policy(hop.pool) for hop in route.hops)
            ),
        )
        self._store(quote)
        return quote

    def _validate_lp(self, intent: LpIntent) -> LpIntent:
        intent = replace(intent, wallet=_address(intent.wallet, "wallet"), pool_id=str(intent.pool_id).lower())
        if not isinstance(intent.op, LpOp):
            raise TxError("invalid_intent", "op must be mint, increase, decrease or collect")
        if not isinstance(intent.slippage_bps, int) or not 0 <= intent.slippage_bps <= 5_000:
            raise TxError("invalid_intent", "slippage_bps must be within 0..5000")
        for name in ("amount0", "amount1", "liquidity"):
            value = getattr(intent, name)
            if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value < 1 << 160:
                raise TxError("invalid_intent", f"{name} must be a non-negative integer below 2**160")
        if intent.token_id is not None and (not isinstance(intent.token_id, int) or intent.token_id < 0):
            raise TxError("invalid_intent", "token_id must be a non-negative integer")
        if intent.op in (LpOp.MINT, LpOp.INCREASE) and intent.amount0 == 0 and intent.amount1 == 0:
            raise TxError("invalid_intent", "adds need amount0 or amount1")
        return intent

    def _position(self, pool: Pool, manager: str, token_id: int, wallet: str, tag: str) -> PositionState:
        owner = "0x" + f"{_word(self._call(manager, SEL_OWNER_OF + token_id.to_bytes(32, 'big'), tag)):040x}"
        if owner != wallet:
            raise TxError("not_owner", "token_id is not owned by wallet")
        if manager == POSM:
            raw = self._call(POSM, posm_pool_and_position(token_id), tag)
            key = PoolKey(
                "0x" + f"{_word(raw, 0):040x}", "0x" + f"{_word(raw, 1):040x}", _word(raw, 2), _signed24(_word(raw, 3)), "0x" + f"{_word(raw, 4):040x}"
            )
            if key.id() != pool.id:
                raise TxError("invalid_intent", "token_id belongs to a different pool")
            info = _word(raw, 5)
            lower, upper = _signed24(info >> 8), _signed24(info >> 32)
            liquidity = _word(self._call(POSM, posm_position_liquidity(token_id), tag))
            return PositionState(lower, upper, liquidity)
        raw = self._call(manager, nfpm_positions(token_id), tag)
        token0, token1, fee = "0x" + f"{_word(raw, 2):040x}", "0x" + f"{_word(raw, 3):040x}", _word(raw, 4)
        if (token0, token1, fee) != (pool.token0, pool.token1, pool.fee_ppm):
            raise TxError("invalid_intent", "token_id belongs to a different pool")
        return PositionState(_signed24(_word(raw, 5)), _signed24(_word(raw, 6)), _word(raw, 7))

    def pool_view(self, pool_id: str, wallet: str, known_ids: tuple[int, ...] = ()) -> dict[str, Any]:
        """Pool state and the wallet's positions in it, all read at one block. ``known_ids``
        are the client's own recent mints; V4 managers are not enumerable and the accounting
        index can lag, so every candidate is verified on chain before it is listed."""
        wallet = _address(wallet, "wallet")
        try:
            pool = self.routes.pool(str(pool_id).lower())
        except IncompletePool as exc:
            raise TxRefusal("incomplete_pool", str(exc)) from exc
        if pool is None:
            raise TxRefusal("unknown_pool", "pool_id is not in the market index")
        manager = self.lps.manager_for(pool)
        if manager is None:
            raise TxRefusal("unsupported_pool", "no position manager for this pool")
        block = self._header("latest")
        tag = hex(block.number)
        if pool.venue is Venue.V4:
            raw = self._call(STATE_VIEW, state_view_slot0(pool.id), tag)
        else:
            raw = self._call(pool.address, selector("slot0()"), tag)
        sqrt_price, tick = _word(raw), _signed24(_word(raw, 1))
        candidates: list[int] = []
        if manager != POSM:
            count = min(_word(self._call(manager, erc20_balance_of(wallet), tag)), 50)
            candidates = [
                _word(self._call(manager, SEL_TOKEN_OF_OWNER_BY_INDEX + bytes.fromhex(wallet[2:].rjust(64, "0")) + index.to_bytes(32, "big"), tag))
                for index in range(count)
            ]
        candidates += self.routes.position_candidates(wallet, pool.id)
        candidates += [int(token_id) for token_id in known_ids[:20]]
        positions = []
        for token_id in dict.fromkeys(candidates):
            try:
                state = self._position(pool, manager, token_id, wallet, tag)
            except (TxError, RuntimeError, ValueError):
                continue
            positions.append({"token_id": str(token_id), "tick_lower": state.tick_lower, "tick_upper": state.tick_upper, "liquidity": str(state.liquidity)})
        tokens = self.balances(wallet, [pool.token0, pool.token1])
        return {
            "pool_id": pool.id, "venue": pool.venue.value, "manager": manager, "fee_ppm": pool.fee_ppm,
            "tick_spacing": pool.tick_spacing, "tick": tick, "sqrt_price_x96": str(sqrt_price), "block": block.to_json(),
            "pons": pool.hook == PONS_HOOK, "hook": pool.hook,
            "token0": {"address": pool.token0, **tokens[pool.token0]}, "token1": {"address": pool.token1, **tokens[pool.token1]},
            "positions": positions,
        }

    def _sqrt_price(self, pool: Pool, tag: str) -> int:
        if pool.venue is Venue.V4:
            return _word(self._call(STATE_VIEW, state_view_slot0(pool.id), tag))
        return _word(self._call(pool.address, selector("slot0()"), tag))

    def _quote_lp(self, intent: LpIntent) -> LpQuote:
        intent = self._validate_lp(intent)
        try:
            pool = self.routes.pool(intent.pool_id)
        except IncompletePool as exc:
            raise TxRefusal("incomplete_pool", str(exc)) from exc
        if pool is None:
            raise TxRefusal("unknown_pool", "pool_id is not in the market index")
        manager = self.lps.manager_for(pool)
        if manager is None:
            raise TxRefusal("unsupported_pool", "no position manager for this pool")
        adding = intent.op in (LpOp.MINT, LpOp.INCREASE)
        if adding and pool.hook == PONS_HOOK:
            raise TxRefusal("pons_add", "adds to Pons pools are refused: the hook keeps every swap fee")
        block = self._header("latest")
        tag = hex(block.number)
        position = None
        if intent.op is not LpOp.MINT:
            if intent.token_id is None:
                raise TxError("invalid_intent", f"{intent.op.value} needs token_id")
            position = self._position(pool, manager, intent.token_id, intent.wallet, tag)
        if adding:
            for currency, amount in ((pool.token0, intent.amount0), (pool.token1, intent.amount1)):
                if amount and self._balance(currency, intent.wallet, tag) < amount:
                    raise TxRefusal("insufficient_balance", f"wallet holds less than the requested {currency}")
        expires_at, deadline = self._times()
        nonces = {}
        if manager == POSM and adding:
            nonces = {t: self._permit_nonce(intent.wallet, t, POSM, tag) for t in (pool.token0, pool.token1) if t != NATIVE}
        sqrt_price = self._sqrt_price(pool, tag)
        plan = self.lps.plan(intent, pool, deadline, sqrt_price_x96=sqrt_price, position=position, permit_nonces=nonces)
        warnings: list[str] = []
        if adding and pool.venue is Venue.V4 and hook_flags(pool.hook) & ADD_GOVERNING_FLAGS:
            warnings.append("hook_governs_adds")
        sim = self.sim.run(intent.wallet, plan.staging, Call(plan.to, plan.calldata(), plan.value), tag)
        if not sim.ok:
            assert sim.revert is not None
            if adding and sim.revert.kind == "hook_reverted":
                raise TxRefusal("hook_blocked_add", sim.revert.detail)
            raise TxRefusal("not_executable", f"{sim.revert.kind} {sim.revert.selector} {sim.revert.detail}".strip())
        amounts = self.lps.amounts(plan.shape, Ledger(sim.logs, traced=True), intent.slippage_bps, sqrt_price)
        quote = LpQuote(
            quote_id=self._quote_id(intent, block),
            wallet=intent.wallet,
            plan=plan,
            block=block,
            expires_at=expires_at,
            deadline=deadline,
            steps=self._steps(intent.wallet, plan, tag),
            warnings=tuple(warnings),
            gas=sim.gas_used,
            intent=intent,
            pool=pool,
            amounts=amounts,
        )
        self._store(quote)
        return quote

    def kind_of(self, quote_id: str) -> str | None:
        with self._lock:
            quote = self._quotes.get(quote_id)
        return None if quote is None else "lp" if isinstance(quote, LpQuote) else "swap"

    def prepare(self, quote_id: str, wallet: str, sigs: Signatures) -> Prepared:
        if not isinstance(quote_id, str) or re.fullmatch(r"[0-9a-f]{64}", quote_id) is None:
            raise TxError("unknown_quote", "quote_id is malformed")
        wallet = _address(wallet, "wallet")
        with self._lock:
            quote = self._quotes.get(quote_id)
        if quote is None:
            raise TxError("unknown_quote", "quote is unknown; re-quote")
        if int(self._clock()) >= quote.expires_at:
            with self._lock:
                self._quotes.pop(quote_id, None)
            raise TxError("expired", "quote expired; re-quote")
        if wallet != quote.wallet:
            raise TxError("wallet_mismatch", "connected wallet is not the quoted wallet")
        self.mismatches = tx_allowlist.verify(self.rpc)
        self._require_enabled()
        if self._header(hex(quote.block.number)).hash != quote.block.hash:
            raise TxError("reorg", "quoted block was reorganized; re-quote")
        for need in quote.plan.approvals:
            if self._allowance(need.token, wallet, need.spender, "latest") < need.required:
                raise TxError("approve_pending", f"approve {need.token} for {need.spender} first")
        if isinstance(quote, SwapQuote):
            plan = self.swaps.finalize(quote.plan, quote.amounts.min_out, sigs)
        else:
            plan = self.lps.finalize(quote.plan, (quote.amounts.bound0, quote.amounts.bound1), sigs)
        data = plan.calldata()
        sim = self.sim.run(wallet, (), Call(plan.to, data, plan.value), "latest")
        if not sim.ok:
            assert sim.revert is not None
            code = sim.revert.kind if sim.revert.kind != "unknown" else "no_longer_executable"
            raise TxError(code, f"{sim.revert.selector} {sim.revert.detail}".strip())
        gas = sim.gas_used * GAS_HEADROOM_PCT // 100
        return Prepared(self._tx(wallet, plan.to, data, plan.value, gas), sim.gas_used, quote.expires_at)

    def _swap_shape(self, wallet: str, data: bytes, value: int) -> SwapShape:
        commands, _ = decode_ur_execute(data)
        amount_in, currency_in = value, NATIVE
        fee: PayPortion | None = None
        fee_leg = FeeLeg.INPUT
        hops: list[Hop] = []
        currency_out: str | None = None
        for command in commands:
            if isinstance(command, Permit2TransferFrom):
                currency_in, amount_in = command.token, command.amount
            elif isinstance(command, PayPortion):
                fee = command
                fee_leg = FeeLeg.OUTPUT if hops else FeeLeg.INPUT
            elif isinstance(command, V3Swap):
                tokens, fees = decode_v3_path(command.path)
                for a, b, fee_ppm in zip(tokens, tokens[1:], fees):
                    pool = self.routes.pool_for(Venue.V3, a, b, fee_ppm)
                    if pool is None:
                        raise TxError("unknown_pool", f"no indexed V3 pool for {a}/{b}")
                    hops.append(Hop(pool, a, b))
            elif isinstance(command, V2Swap):
                for a, b in zip(command.path, command.path[1:]):
                    pool = self.routes.pool_for(Venue.V2, a, b)
                    if pool is None:
                        raise TxError("unknown_pool", f"no indexed V2 pool for {a}/{b}")
                    hops.append(Hop(pool, a, b))
            elif isinstance(command, V4Swap):
                single = next(p for p in command.params if isinstance(p, V4SwapExactInSingle))
                key = single.key
                pool = v4_pool(key)
                cin, cout = (key.currency0, key.currency1) if single.zero_for_one else (key.currency1, key.currency0)
                hops.append(Hop(pool, cin, cout))
            elif isinstance(command, Sweep) and command.recipient == MSG_SENDER:
                currency_out = command.token
            elif isinstance(command, UnwrapWeth) and command.recipient == MSG_SENDER:
                currency_out = NATIVE
        if fee is None or not hops:
            raise TxError("unknown_target", "calldata is not an rhpools swap")
        route = Route(tuple(hops))
        return SwapShape(
            wallet=wallet,
            route=route,
            currency_in=currency_in,
            currency_out=currency_out or route.currency_out,
            amount_in=amount_in,
            fee_leg=fee_leg,
            fee_currency=fee.token,
            fee_recipient=fee.recipient,
            fee_bps=fee.bips,
        )

    def _router_swap_shape(self, wallet: str, to: str, data: bytes, value: int) -> SwapShape:
        pancake = to == PANCAKE_SMART_ROUTER
        calls = decode(["uint256", "bytes[]"], data[4:])[1] if pancake and data[:4] == SEL_ROUTER_MULTICALL_DEADLINE else decode_multicall(data)
        if len(calls) != 2:
            raise TxError("unknown_target", "router swap must contain swap and fee sweep")
        swap, sweep = calls
        if pancake and swap[:4] == SEL_PANCAKE_EXACT_INPUT_SINGLE:
            token_in, token_out, fee, _, amount_in, _, _ = decode(
                ["address", "address", "uint24", "address", "uint256", "uint256", "uint160"], swap[4:],
            )
            tokens, fees = (token_in, token_out), (fee,)
        elif to == SLIPSTREAM_SWAP_ROUTER and swap[:4] == SEL_SLIPSTREAM_EXACT_INPUT_SINGLE:
            token_in, token_out, spacing, _, _, amount_in, _, _ = decode(
                ["address", "address", "int24", "address", "uint256", "uint256", "uint256", "uint160"], swap[4:],
            )
            tokens, fees = (token_in, token_out), (spacing,)
        elif not pancake and swap[:4] == SEL_V3_EXACT_INPUT_SINGLE:
            token_in, token_out, fee, _, _, amount_in, _, _ = decode(
                ["address", "address", "uint24", "address", "uint256", "uint256", "uint256", "uint160"], swap[4:],
            )
            tokens, fees = (token_in, token_out), (fee,)
        elif pancake and swap[:4] == SEL_PANCAKE_EXACT_INPUT:
            (path, _, amount_in, _), = decode(["(bytes,address,uint256,uint256)"], swap[4:])
            tokens, fees = decode_v3_path(path)
        elif not pancake and swap[:4] == SEL_V3_EXACT_INPUT:
            (path, _, _, amount_in, _), = decode(
                ["(bytes,address,uint256,uint256,uint256)"], swap[4:],
            )
            tokens, fees = decode_v3_path(path)
        else:
            raise TxError("unknown_target", "router calldata has no supported swap")
        factory = PANCAKE_V3_FACTORY if pancake else SLIPSTREAM_FACTORY if to == SLIPSTREAM_SWAP_ROUTER else GIGA_V3_FACTORY
        hops = []
        for a, b, fee in zip(tokens, tokens[1:], fees):
            pool = self.routes.pool_for(Venue.V3, a, b, factory=factory, **(
                {"tick_spacing": fee} if to == SLIPSTREAM_SWAP_ROUTER else {"fee_ppm": fee}
            ))
            if pool is None:
                raise TxError("unknown_pool", f"no indexed pool for {a}/{b}")
            hops.append(Hop(pool, a, b))
        if sweep[:4] == SEL_UNWRAP_WITH_FEE:
            _, recipient, bps, fee_recipient = decode(["uint256", "address", "uint256", "address"], sweep[4:])
            currency_out = NATIVE
        elif sweep[:4] == SEL_SWEEP_WITH_FEE:
            currency_out, _, recipient, bps, fee_recipient = decode(
                ["address", "uint256", "address", "uint256", "address"], sweep[4:],
            )
        else:
            raise TxError("unknown_target", "router calldata has no fee sweep")
        if recipient.lower() != wallet or not (currency_out == tokens[-1] or currency_out == NATIVE and tokens[-1] == WETH):
            raise TxError("unknown_target", "router settlement differs from swap")
        currency_in = NATIVE if value else tokens[0]
        if value and value != amount_in:
            raise TxError("unknown_target", "router input value differs from swap")
        return SwapShape(wallet, Route(tuple(hops)), currency_in, currency_out, amount_in,
                         FeeLeg.OUTPUT, currency_out, fee_recipient.lower(), bps)

    def _lp_shape(self, wallet: str, to: str, data: bytes, tag: str) -> LpShape:
        calls = decode_multicall(data) if data[:4] == SEL_MULTICALL else (data,)
        if to == POSM:
            modify = next(c for c in calls if c[:4] != SEL_POSM_PERMIT_BATCH)
            params, _ = decode_posm_modify_liquidities(modify)
            first = params[0]
            if isinstance(first, PosmMint):
                key = first.key
                pool = v4_pool(key)
                return LpShape(wallet, LpOp.MINT, pool, POSM, None, first.tick_lower, first.tick_upper, first.liquidity)
            if isinstance(first, (PosmIncrease, PosmDecrease)):
                raw = self._call(POSM, posm_pool_and_position(first.token_id), tag)
                key = PoolKey("0x" + f"{_word(raw, 0):040x}", "0x" + f"{_word(raw, 1):040x}", _word(raw, 2), _signed24(_word(raw, 3)), "0x" + f"{_word(raw, 4):040x}")
                pool = v4_pool(key)
                info = _word(raw, 5)
                if isinstance(first, PosmIncrease):
                    op = LpOp.INCREASE
                else:
                    op = LpOp.DECREASE if first.liquidity else LpOp.COLLECT
                return LpShape(wallet, op, pool, POSM, first.token_id, _signed24(info >> 8), _signed24(info >> 32), first.liquidity)
            raise TxError("unknown_target", "calldata is not an rhpools lp action")
        factory = NFPM_FACTORY[to]
        call = decode_nfpm_call(calls[0])
        if isinstance(call, NfpmMint):
            pool = self.routes.pool_for(Venue.V3, call.token0, call.token1, call.fee, factory=factory)
            if pool is None:
                raise TxError("unknown_pool", "no indexed pool for the minted pair")
            return LpShape(wallet, LpOp.MINT, pool, to, None, call.tick_lower, call.tick_upper, 0)
        raw = self._call(to, nfpm_positions(call.token_id), tag)
        token0, token1, fee = "0x" + f"{_word(raw, 2):040x}", "0x" + f"{_word(raw, 3):040x}", _word(raw, 4)
        pool = self.routes.pool_for(Venue.V3, token0, token1, fee, factory=factory)
        if pool is None:
            raise TxError("unknown_pool", "no indexed pool for the position")
        lower, upper = _signed24(_word(raw, 5)), _signed24(_word(raw, 6))
        if isinstance(call, NfpmIncrease):
            return LpShape(wallet, LpOp.INCREASE, pool, to, call.token_id, lower, upper, 0)
        if isinstance(call, NfpmDecrease):
            return LpShape(wallet, LpOp.DECREASE, pool, to, call.token_id, lower, upper, call.liquidity)
        if isinstance(call, NfpmCollect):
            return LpShape(wallet, LpOp.COLLECT, pool, to, call.token_id, lower, upper, 0)
        raise TxError("unknown_target", "calldata is not an rhpools lp action")

    def receipt(self, tx_hash: str, wallet: str) -> Fill:
        if not isinstance(tx_hash, str) or _HASH_RE.fullmatch(tx_hash.lower()) is None:
            raise TxError("unknown_tx", "hash is malformed")
        tx_hash = tx_hash.lower()
        wallet = _address(wallet, "wallet")
        tx = self.rpc.call("eth_getTransactionByHash", [tx_hash])
        if not isinstance(tx, dict):
            raise TxError("unknown_tx", "transaction is unknown to the node")
        if str(tx.get("from", "")).lower() != wallet:
            raise TxError("wallet_mismatch", "transaction was not sent by wallet")
        to = str(tx.get("to", "")).lower()
        data = bytes.fromhex(str(tx.get("input", "0x"))[2:])
        value = _hex_int(tx.get("value", "0x0"), "value")
        receipt = self.rpc.call("eth_getTransactionReceipt", [tx_hash])
        if not isinstance(receipt, dict):
            return Fill(tx_hash, "pending", None, None, None, None)
        block = _hex_int(receipt.get("blockNumber"), "blockNumber")
        gas_used = _hex_int(receipt.get("gasUsed"), "gasUsed")
        gas_cost = gas_used * _hex_int(receipt.get("effectiveGasPrice", "0x0"), "effectiveGasPrice")
        if receipt.get("status") != "0x1":
            return Fill(tx_hash, "failed", block, None, gas_used, gas_cost)
        logs = list(receipt.get("logs", []))
        tag = hex(block)
        if to == UR:
            amounts: Amounts | LpAmounts = swap_amounts(self._swap_shape(wallet, data, value), Ledger(logs, traced=False), self.routes.hook_policy)
        elif to in (GIGA_SWAP_ROUTER, PANCAKE_SMART_ROUTER, SLIPSTREAM_SWAP_ROUTER):
            shape = self._router_swap_shape(wallet, to, data, value)
            native_flows = {}
            if shape.currency_out == NATIVE:
                before = _hex_int(self.rpc.call("eth_getBalance", [wallet, hex(block - 1)]), "balance")
                after = _hex_int(self.rpc.call("eth_getBalance", [wallet, tag]), "balance")
                native_flows[wallet] = after - before + gas_cost + value
            amounts = swap_amounts(shape, Ledger(logs, traced=False, native_flows=native_flows), self.routes.hook_policy)
        elif to in LP_TARGETS:
            shape = self._lp_shape(wallet, to, data, tag)
            before = _hex_int(self.rpc.call("eth_getBalance", [wallet, hex(block - 1)]), "balance")
            after = _hex_int(self.rpc.call("eth_getBalance", [wallet, tag]), "balance")
            amounts = self.lps.amounts(shape, Ledger(logs, traced=False, native_flows={wallet: after - before + gas_cost}), 0, None)
        else:
            raise TxError("unknown_target", "transaction target is not an rhpools contract")
        return Fill(tx_hash, "confirmed", block, amounts, gas_used, gas_cost)


__all__ = [
    "Block", "Fill", "LpQuote", "Prepared", "Quote", "QuoteBase", "SimResult", "Simulator", "Step",
    "SwapQuote", "TxCore",
]
