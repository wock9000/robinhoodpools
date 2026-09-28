"""A swap Plan is a tuple of typed UniversalRouter commands; ``min_out`` lives in
exactly one command and the Permit2 permit is prepended at finalize. LP plans
target a V3 NFPM or the V4 PositionManager. Accounting reads venue Swap logs,
ERC-20 Transfer logs and (under eth_simulateV1 traceTransfers) native
pseudo-transfers, then reconciles them against the plan to the wei.
"""
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any
from eth_abi import encode

from .lp_math import Q96, amount0_delta, amount1_delta, sqrt_ratio_at_tick
from .tx_chain import (
    ADDRESS_THIS, CONTRACT_BALANCE, MAX_UINT128, MAX_UINT256, MSG_SENDER, NATIVE,
    NFPM_BY_FACTORY, OPEN_DELTA, PANCAKE_SMART_ROUTER, PERMIT2, POOL_MANAGER, POSM,
    SLIPSTREAM_SWAP_ROUTER, V3_ROUTER_BY_FACTORY,
    TOPIC_NFPM_COLLECT, TOPIC_NFPM_DECREASE, TOPIC_NFPM_INCREASE, TOPIC_PANCAKE_SWAP,
    TOPIC_TRANSFER, TOPIC_V2_SWAP, TOPIC_V3_SWAP, TOPIC_V4_MODIFY_LIQUIDITY, TOPIC_V4_SWAP, UR, WETH, NfpmCollect,
    NfpmDecrease, NfpmIncrease, NfpmMint, PayPortion, Permit2Permit, Permit2TransferFrom,
    PermitBatch, PermitDetails, PermitSingle, PosmDecrease, PosmIncrease, PosmMint, PosmParam,
    PosmSettlePair, PosmSweep, PosmTakePair, Sweep, UnwrapWeth, UrCommand, V2Swap, V3Swap,
    V4Settle, V4Swap, V4SwapExactInSingle, V4Take, WrapEth, erc20_approve, multicall,
    permit2_approve, permit_batch_typed_data, permit_single_typed_data, posm_modify_liquidities,
    posm_permit_batch, SEL_ROUTER_MULTICALL_DEADLINE, ur_execute, v3_path, v3_router_fee, v3_router_swap,
)
from .tx_routes import Hop, HookPolicy, Pool, Route, Side, Venue, same_asset


class TxError(ValueError):
    """A request or state error the caller can act on; ``code`` is a closed vocabulary."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail


class TxRefusal(TxError):
    """The quote was refused; the ticket renders ``code`` as the refusal state."""


class FeeLeg(str, Enum):
    INPUT = "input"
    OUTPUT = "output"


class LpOp(str, Enum):
    MINT = "mint"
    INCREASE = "increase"
    DECREASE = "decrease"
    COLLECT = "collect"


@dataclass(frozen=True)
class TxPolicy:
    fee_bps: int
    fee_recipient: str
    max_impact_bps: int = 1500
    quote_ttl_s: int = 60


@dataclass(frozen=True)
class SwapIntent:
    wallet: str
    side: Side
    token: str
    quote_currency: str
    amount_in: int
    slippage_bps: int

    @property
    def currency_in(self) -> str:
        return self.quote_currency if self.side is Side.BUY else self.token

    @property
    def currency_out(self) -> str:
        return self.token if self.side is Side.BUY else self.quote_currency

    @property
    def fee_leg(self) -> FeeLeg:
        return FeeLeg.INPUT if self.side is Side.BUY else FeeLeg.OUTPUT

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "swap",
            "wallet": self.wallet,
            "side": self.side.value,
            "token": self.token,
            "quote_currency": self.quote_currency,
            "amount_in": str(self.amount_in),
            "slippage_bps": self.slippage_bps,
        }


@dataclass(frozen=True)
class LpIntent:
    wallet: str
    op: LpOp
    pool_id: str
    slippage_bps: int
    tick_lower: int = 0
    tick_upper: int = 0
    amount0: int = 0
    amount1: int = 0
    liquidity: int = 0
    token_id: int | None = None

    def to_json(self) -> dict[str, Any]:
        return {
            "kind": "lp",
            "wallet": self.wallet,
            "op": self.op.value,
            "pool_id": self.pool_id,
            "slippage_bps": self.slippage_bps,
            "tick_lower": self.tick_lower,
            "tick_upper": self.tick_upper,
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "liquidity": str(self.liquidity),
            "token_id": self.token_id,
        }


Intent = SwapIntent | LpIntent


@dataclass(frozen=True)
class ApprovalNeed:
    token: str
    spender: str
    approve_amount: int
    required: int


@dataclass(frozen=True)
class Call:
    to: str
    data: bytes
    value: int = 0


@dataclass(frozen=True)
class Signatures:
    permit: bytes | None = None


@dataclass(frozen=True)
class Fee:
    currency: str
    amount: int

    def to_json(self) -> dict[str, Any]:
        return {"currency": self.currency, "amount": str(self.amount)}


@dataclass(frozen=True)
class SwapShape:
    wallet: str
    route: Route
    currency_in: str
    currency_out: str
    amount_in: int
    fee_leg: FeeLeg
    fee_currency: str
    fee_recipient: str
    fee_bps: int


@dataclass(frozen=True)
class LpShape:
    wallet: str
    op: LpOp
    pool: Pool
    manager: str
    token_id: int | None
    tick_lower: int
    tick_upper: int
    liquidity: int


@dataclass(frozen=True)
class SwapBody:
    commands: tuple[UrCommand, ...]
    min_out_index: int
    shape: SwapShape



@dataclass(frozen=True)
class RouterBody:
    shape: SwapShape
    gross_min_out: int = 0

@dataclass(frozen=True)
class NfpmBody:
    call: NfpmMint | NfpmIncrease | NfpmDecrease | NfpmCollect
    collect: NfpmCollect | None
    shape: LpShape


@dataclass(frozen=True)
class PosmBody:
    params: tuple[PosmParam, ...]
    shape: LpShape


@dataclass(frozen=True)
class Plan:
    to: str
    value: int
    deadline: int
    body: SwapBody | RouterBody | NfpmBody | PosmBody
    approvals: tuple[ApprovalNeed, ...]
    permit: PermitSingle | PermitBatch | None
    staging: tuple[Call, ...]
    signature: bytes | None = None

    def calldata(self) -> bytes:
        body = self.body
        if isinstance(body, SwapBody):
            commands = body.commands
            if self.signature is not None and isinstance(self.permit, PermitSingle):
                commands = (Permit2Permit(self.permit, self.signature), *commands)
            return ur_execute(commands, self.deadline)
        if isinstance(body, NfpmBody):
            if body.collect is not None:
                return multicall((body.call.encode(), body.collect.encode()))
            return body.call.encode()
        if isinstance(body, RouterBody):
            hops = body.shape.route.hops
            tokens = (hops[0].currency_in, *(hop.currency_out for hop in hops))
            fees = tuple(hop.pool.tick_spacing if self.to == SLIPSTREAM_SWAP_ROUTER else hop.pool.fee_ppm for hop in hops)
            output = body.shape.currency_out
            pancake = self.to == PANCAKE_SMART_ROUTER
            calls = (
                v3_router_swap(tokens, fees, body.shape.amount_in, self.deadline, self.to, pancake=pancake,
                               slipstream=self.to == SLIPSTREAM_SWAP_ROUTER),
                v3_router_fee(output, body.gross_min_out, body.shape.wallet, body.shape.fee_bps, body.shape.fee_recipient),
            )
            return SEL_ROUTER_MULTICALL_DEADLINE + encode(["uint256", "bytes[]"], [self.deadline, calls]) if pancake else multicall(calls)
        modify = posm_modify_liquidities(body.params, self.deadline)
        if self.signature is not None and isinstance(self.permit, PermitBatch):
            return multicall((posm_permit_batch(body.shape.wallet, self.permit, self.signature), modify))
        return modify

    def permit_typed_data(self) -> dict[str, Any] | None:
        if isinstance(self.permit, PermitSingle):
            return permit_single_typed_data(self.permit)
        if isinstance(self.permit, PermitBatch):
            return permit_batch_typed_data(self.permit)
        return None

    @property
    def shape(self) -> SwapShape | LpShape:
        return self.body.shape


@dataclass(frozen=True)
class Amounts:
    amount_in: int
    pool_out: int
    hook_fee: Fee | None
    creator_tax: Fee | None
    rhpools_fee: Fee
    net_out: int
    min_out: int
    impact_bps: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "amount_in": str(self.amount_in),
            "pool_out": str(self.pool_out),
            "hook_fee": self.hook_fee.to_json() if self.hook_fee else None,
            "creator_tax": self.creator_tax.to_json() if self.creator_tax else None,
            "rhpools_fee": self.rhpools_fee.to_json(),
            "net_out": str(self.net_out),
            "min_out": str(self.min_out),
            "impact_bps": self.impact_bps,
        }


@dataclass(frozen=True)
class LpAmounts:
    liquidity: int
    amount0: int
    amount1: int
    fees0: int | None
    fees1: int | None
    token_id: int | None
    bound0: int
    bound1: int

    def to_json(self) -> dict[str, Any]:
        return {
            "liquidity": str(self.liquidity),
            "amount0": str(self.amount0),
            "amount1": str(self.amount1),
            "fees0": None if self.fees0 is None else str(self.fees0),
            "fees1": None if self.fees1 is None else str(self.fees1),
            "token_id": self.token_id,
            "bound0": str(self.bound0),
            "bound1": str(self.bound1),
        }


def _hex_int(value: Any) -> int:
    return int(value, 16) if isinstance(value, str) else int(value)


def _signed(value: int, bits: int) -> int:
    return value - (1 << bits) if value >= 1 << (bits - 1) else value


def _topic_address(word: str) -> str:
    return "0x" + word[-40:].lower()


def _words(data: str) -> list[int]:
    body = data[2:] if data.startswith("0x") else data
    return [int(body[i:i + 64], 16) for i in range(0, len(body), 64)]


NATIVE_TRACE = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"


@dataclass(frozen=True)
class SwapLog:
    pool_key: str
    amount0: int
    amount1: int
    venue: Venue


class Ledger:
    def __init__(self, logs: list[dict[str, Any]], *, traced: bool, native_flows: dict[str, int] | None = None) -> None:
        """``traced`` says the logs came from eth_simulateV1 with traceTransfers, so native moves are complete."""
        self.transfers: list[tuple[str, str, str, int]] = []
        self.swaps: list[SwapLog] = []
        self.nft_mints: list[tuple[str, str, int]] = []
        self.nfpm_events: list[tuple[str, str, int, int, int, int]] = []
        self.liquidity_deltas: list[tuple[str, int]] = []
        self.native_traced = traced
        self._native_flows = {k.lower(): v for k, v in (native_flows or {}).items()}
        for log in logs:
            address = str(log["address"]).lower()
            topics = [str(t).lower() for t in log.get("topics", [])]
            data = str(log.get("data", "0x"))
            if not topics:
                continue
            sig = topics[0]
            if sig == TOPIC_TRANSFER and len(topics) == 3:
                amount = _words(data)[0] if len(data) > 2 else 0
                if address == NATIVE_TRACE:
                    address = NATIVE
                self.transfers.append((address, _topic_address(topics[1]), _topic_address(topics[2]), amount))
            elif sig == TOPIC_TRANSFER and len(topics) == 4 and _topic_address(topics[1]) == NATIVE:
                self.nft_mints.append((address, _topic_address(topics[2]), int(topics[3], 16)))
            elif sig == TOPIC_V4_SWAP and address == POOL_MANAGER:
                w = _words(data)
                self.swaps.append(SwapLog(topics[1], _signed(w[0], 256), _signed(w[1], 256), Venue.V4))
            elif sig in (TOPIC_V3_SWAP, TOPIC_PANCAKE_SWAP):
                w = _words(data)
                self.swaps.append(SwapLog(address, _signed(w[0], 256), _signed(w[1], 256), Venue.V3))
            elif sig == TOPIC_V2_SWAP:
                w = _words(data)
                self.swaps.append(SwapLog(address, w[2] - w[0], w[3] - w[1], Venue.V2))
            elif sig in (TOPIC_NFPM_INCREASE, TOPIC_NFPM_DECREASE):
                w = _words(data)
                self.nfpm_events.append((address, sig, int(topics[1], 16), w[0], w[1], w[2]))
            elif sig == TOPIC_NFPM_COLLECT:
                w = _words(data)
                self.nfpm_events.append((address, sig, int(topics[1], 16), 0, w[1], w[2]))
            elif sig == TOPIC_V4_MODIFY_LIQUIDITY and address == POOL_MANAGER:
                w = _words(data)
                self.liquidity_deltas.append((topics[1], _signed(w[2], 256)))

    def observable(self, currency: str) -> bool:
        return currency != NATIVE or self.native_traced

    def received(self, currency: str, who: str) -> int:
        who = who.lower()
        return sum(a for c, f, t, a in self.transfers if c == currency and t == who and f != who)

    def sent(self, currency: str, who: str) -> int:
        who = who.lower()
        return sum(a for c, f, t, a in self.transfers if c == currency and f == who and t != who)

    def flow(self, currency: str, who: str) -> int:
        """Native falls back to the supplied balance delta when the logs were not traced."""
        if currency == NATIVE and not self.native_traced:
            try:
                return self._native_flows[who.lower()]
            except KeyError:
                raise TxError("unobservable", f"no native flow for {who}") from None
        return self.received(currency, who) - self.sent(currency, who)

    def hop_io(self, hop: Hop) -> tuple[int, int]:
        key = hop.pool.id if hop.pool.venue is Venue.V4 else hop.pool.address
        matches = [s for s in self.swaps if s.pool_key == key]
        if len(matches) != 1:
            raise TxError("unmodeled_fee", f"expected one Swap log for {key[:10]}, saw {len(matches)}")
        swap = matches[0]
        zero_in = hop.currency_in == hop.pool.token0
        a_in, a_out = (swap.amount0, swap.amount1) if zero_in else (swap.amount1, swap.amount0)
        if swap.venue is Venue.V3:
            return a_in, -a_out
        if swap.venue is Venue.V2:
            return -a_in, a_out
        return -a_in, a_out


def _floor_bps(amount: int, bps: int) -> int:
    return amount * bps // 10_000


def swap_amounts(shape: SwapShape, ledger: Ledger, hook_policy: Callable[[Pool], HookPolicy]) -> Amounts:
    """Reconcile the plan against logs; raise ``unmodeled_fee`` on any wei of disagreement."""
    fee_in = _floor_bps(shape.amount_in, shape.fee_bps) if shape.fee_leg is FeeLeg.INPUT else 0
    carry = shape.amount_in - fee_in
    hook_fee = creator_tax = None
    pool_out = 0
    for hop in shape.route.hops:
        hop_in, hop_out = ledger.hop_io(hop)
        if hop_in != carry:
            raise TxError("unmodeled_fee", f"hop input {hop_in} != carried {carry}")
        pool_out = hop_out
        policy = hook_policy(hop.pool)
        carry = hop_out
        if policy.is_pons:
            hook_fee = Fee(hop.currency_out, _floor_bps(hop_out, policy.hook_fee_bps))
            creator_tax = Fee(hop.currency_out, _floor_bps(hop_out, policy.creator_tax_bps))
            carry -= hook_fee.amount + creator_tax.amount
    if shape.fee_leg is FeeLeg.OUTPUT:
        fee_amount = _floor_bps(carry, shape.fee_bps)
        net_out = carry - fee_amount
    else:
        fee_amount = fee_in
        net_out = carry
    rhpools_fee = Fee(shape.fee_currency, fee_amount)
    if ledger.observable(shape.fee_currency):
        got = ledger.received(shape.fee_currency, shape.fee_recipient)
        if got != fee_amount:
            raise TxError("unmodeled_fee", f"fee recipient received {got}, expected {fee_amount}")
    if ledger.observable(shape.currency_out):
        got = ledger.flow(shape.currency_out, shape.wallet)
        if got != net_out:
            raise TxError("unmodeled_fee", f"wallet received {got}, expected {net_out}")
    if shape.currency_in != NATIVE:
        spent = ledger.sent(shape.currency_in, shape.wallet)
        if spent != shape.amount_in:
            raise TxError("unmodeled_fee", f"wallet spent {spent}, expected {shape.amount_in}")
    return Amounts(shape.amount_in, pool_out, hook_fee, creator_tax, rhpools_fee, net_out, 0, None)


def min_out_for(net_out: int, slippage_bps: int) -> int:
    return net_out * (10_000 - slippage_bps) // 10_000


def impact_bps(full: Amounts, small: Amounts) -> int | None:
    if small.net_out == 0 or full.amount_in == 0:
        return None
    ratio = 10_000 * full.net_out * small.amount_in // (full.amount_in * small.net_out)
    return max(0, 10_000 - ratio)


def _with_min_out(command: UrCommand, value: int) -> UrCommand:
    if isinstance(command, (V3Swap, V2Swap)):
        return replace(command, min_out=value)
    if isinstance(command, (Sweep, UnwrapWeth)):
        return replace(command, amount_min=value)
    if isinstance(command, V4Swap):
        params = tuple(replace(p, min_out=value) if isinstance(p, V4SwapExactInSingle) else p for p in command.params)
        return replace(command, params=params)
    raise TxError("internal", "command carries no min_out")


def _hop_command(hop: Hop, recipient: str, min_out: int) -> UrCommand:
    pool = hop.pool
    if pool.venue is Venue.V3:
        return V3Swap(recipient, CONTRACT_BALANCE, min_out, v3_path((hop.currency_in, hop.currency_out), (pool.fee_ppm,)), False)
    if pool.venue is Venue.V2:
        return V2Swap(recipient, CONTRACT_BALANCE, min_out, (hop.currency_in, hop.currency_out), False)
    return V4Swap((
        V4Settle(hop.currency_in, CONTRACT_BALANCE, False),
        V4SwapExactInSingle(pool.key, hop.currency_in == pool.token0, OPEN_DELTA, min_out),
        V4Take(hop.currency_out, recipient, OPEN_DELTA),
    ))


class SwapPlanner:
    def plan(self, intent: SwapIntent, route: Route, policy: TxPolicy, deadline: int, permit_nonce: int) -> Plan:
        if not same_asset(route.currency_in, intent.currency_in) or not same_asset(route.currency_out, intent.currency_out):
            raise TxError("internal", "route does not serve the intent")
        factory = route.hops[0].pool.factory
        if factory in V3_ROUTER_BY_FACTORY and all(
            h.pool.venue is Venue.V3 and h.pool.factory == factory for h in route.hops
        ):
            router = V3_ROUTER_BY_FACTORY[factory]
            shape = SwapShape(
                intent.wallet, route, intent.currency_in, intent.currency_out, intent.amount_in,
                FeeLeg.OUTPUT, intent.currency_out, policy.fee_recipient, policy.fee_bps,
            )
            approvals = () if intent.currency_in == NATIVE else (
                ApprovalNeed(intent.currency_in, router, intent.amount_in, intent.amount_in),
            )
            staging = tuple(Call(a.token, erc20_approve(a.spender, a.approve_amount)) for a in approvals)
            return Plan(router, intent.amount_in if intent.currency_in == NATIVE else 0,
                        deadline, RouterBody(shape), approvals, None, staging)
        currency_in, currency_out = intent.currency_in, intent.currency_out
        commands: list[UrCommand] = []
        held = currency_in
        value = 0
        permit: PermitSingle | None = None
        approvals: tuple[ApprovalNeed, ...] = ()
        staging: tuple[Call, ...] = ()
        if currency_in == NATIVE:
            value = intent.amount_in
        else:
            commands.append(Permit2TransferFrom(currency_in, ADDRESS_THIS, intent.amount_in))
            permit = PermitSingle(PermitDetails(currency_in, intent.amount_in, deadline, permit_nonce), UR, deadline)
            approvals = (ApprovalNeed(currency_in, PERMIT2, MAX_UINT256, intent.amount_in),)
            staging = (
                Call(currency_in, erc20_approve(PERMIT2, MAX_UINT256)),
                Call(PERMIT2, permit2_approve(currency_in, UR, intent.amount_in, deadline)),
            )
        fee_currency = held
        if intent.fee_leg is FeeLeg.INPUT:
            commands.append(PayPortion(held, policy.fee_recipient, policy.fee_bps))
        last = len(route.hops) - 1
        min_out_index = -1
        for index, hop in enumerate(route.hops):
            if hop.currency_in != held:
                commands.append(WrapEth(ADDRESS_THIS, CONTRACT_BALANCE) if held == NATIVE else UnwrapWeth(ADDRESS_THIS, 0))
                held = hop.currency_in
            direct = index == last and intent.fee_leg is FeeLeg.INPUT and hop.currency_out == currency_out
            commands.append(_hop_command(hop, MSG_SENDER if direct else ADDRESS_THIS, 0))
            if direct:
                min_out_index = len(commands) - 1
            held = hop.currency_out
        if intent.fee_leg is FeeLeg.OUTPUT:
            if currency_out == WETH and held == NATIVE:
                commands.append(WrapEth(ADDRESS_THIS, CONTRACT_BALANCE))
                held = WETH
            fee_currency = held
            commands.append(PayPortion(held, policy.fee_recipient, policy.fee_bps))
            if currency_out == NATIVE and held == WETH:
                commands.append(UnwrapWeth(MSG_SENDER, 0))
            else:
                commands.append(Sweep(held, MSG_SENDER, 0))
            min_out_index = len(commands) - 1
        if min_out_index < 0:
            raise TxError("internal", "plan has no min_out slot")
        shape = SwapShape(
            wallet=intent.wallet,
            route=route,
            currency_in=currency_in,
            currency_out=currency_out,
            amount_in=intent.amount_in,
            fee_leg=intent.fee_leg,
            fee_currency=fee_currency,
            fee_recipient=policy.fee_recipient,
            fee_bps=policy.fee_bps,
        )
        return Plan(UR, value, deadline, SwapBody(tuple(commands), min_out_index, shape), approvals, permit, staging)

    def finalize(self, plan: Plan, min_out: int, sigs: Signatures) -> Plan:
        body = plan.body
        if isinstance(body, RouterBody):
            if min_out and min_out > 0 and body.shape.fee_bps >= 10_000:
                raise TxError("internal", "invalid fee")
            gross_min = (min_out * 10_000 + (10_000 - body.shape.fee_bps) - 1) // (10_000 - body.shape.fee_bps)
            return replace(plan, body=replace(body, gross_min_out=gross_min))
        if not isinstance(body, SwapBody):
            raise TxError("internal", "not a swap plan")
        if plan.permit is not None and sigs.permit is None:
            raise TxError("permit_required", "the quote needs a Permit2 signature")
        commands = list(body.commands)
        commands[body.min_out_index] = _with_min_out(commands[body.min_out_index], min_out)
        return replace(plan, body=replace(body, commands=tuple(commands)), signature=sigs.permit)


def liquidity_for_amounts(sqrt_p: int, sqrt_a: int, sqrt_b: int, amount0: int, amount1: int) -> int:
    if sqrt_a > sqrt_b:
        sqrt_a, sqrt_b = sqrt_b, sqrt_a

    def liq0(lo: int, hi: int) -> int:
        return amount0 * (lo * hi // Q96) // (hi - lo)

    def liq1(lo: int, hi: int) -> int:
        return amount1 * Q96 // (hi - lo)

    if sqrt_p <= sqrt_a:
        return liq0(sqrt_a, sqrt_b)
    if sqrt_p < sqrt_b:
        return min(liq0(sqrt_p, sqrt_b), liq1(sqrt_a, sqrt_p))
    return liq1(sqrt_a, sqrt_b)


def principal_for_liquidity(sqrt_p: int, lower: int, upper: int, liquidity: int, *, round_up: bool) -> tuple[int, int]:
    sqrt_a, sqrt_b = sqrt_ratio_at_tick(lower), sqrt_ratio_at_tick(upper)
    if sqrt_p <= sqrt_a:
        return amount0_delta(sqrt_a, sqrt_b, liquidity, round_up=round_up), 0
    if sqrt_p < sqrt_b:
        return (
            amount0_delta(sqrt_p, sqrt_b, liquidity, round_up=round_up),
            amount1_delta(sqrt_a, sqrt_p, liquidity, round_up=round_up),
        )
    return 0, amount1_delta(sqrt_a, sqrt_b, liquidity, round_up=round_up)


@dataclass(frozen=True)
class PositionState:
    tick_lower: int
    tick_upper: int
    liquidity: int


class LpPlanner:
    def manager_for(self, pool: Pool) -> str | None:
        if pool.venue is Venue.V4:
            return POSM
        if pool.venue is Venue.V3:
            return NFPM_BY_FACTORY.get(pool.factory)
        return None

    def plan(
        self,
        intent: LpIntent,
        pool: Pool,
        deadline: int,
        *,
        sqrt_price_x96: int,
        position: PositionState | None,
        permit_nonces: dict[str, int],
    ) -> Plan:
        manager = self.manager_for(pool)
        if manager is None:
            raise TxRefusal("unsupported_pool", "no position manager for this pool")
        if intent.op in (LpOp.INCREASE, LpOp.DECREASE, LpOp.COLLECT):
            if intent.token_id is None or position is None:
                raise TxError("invalid_intent", f"{intent.op.value} needs token_id")
            lower, upper = position.tick_lower, position.tick_upper
        else:
            lower, upper = intent.tick_lower, intent.tick_upper
            if not lower < upper or pool.tick_spacing <= 0 or lower % pool.tick_spacing or upper % pool.tick_spacing:
                raise TxError("invalid_intent", "ticks must be ordered multiples of the pool tick spacing")
        shape = LpShape(intent.wallet, intent.op, pool, manager, intent.token_id, lower, upper, intent.liquidity)
        if manager == POSM:
            return self._posm(intent, pool, deadline, shape, sqrt_price_x96, position, permit_nonces)
        return self._nfpm(intent, pool, deadline, shape, manager, position)

    def _nfpm(self, intent: LpIntent, pool: Pool, deadline: int, shape: LpShape, manager: str, position: PositionState | None) -> Plan:
        wallet = intent.wallet
        approvals: tuple[ApprovalNeed, ...] = ()
        collect: NfpmCollect | None = None
        if intent.op is LpOp.MINT:
            call: Any = NfpmMint(pool.token0, pool.token1, pool.fee_ppm, shape.tick_lower, shape.tick_upper, intent.amount0, intent.amount1, 0, 0, wallet, deadline)
        elif intent.op is LpOp.INCREASE:
            call = NfpmIncrease(intent.token_id, intent.amount0, intent.amount1, 0, 0, deadline)
        elif intent.op is LpOp.DECREASE:
            assert position is not None
            if not 0 < intent.liquidity <= position.liquidity:
                raise TxError("invalid_intent", "liquidity must be within the position")
            call = NfpmDecrease(intent.token_id, intent.liquidity, 0, 0, deadline)
            collect = NfpmCollect(intent.token_id, wallet)
        else:
            call = NfpmCollect(intent.token_id, wallet)
        if intent.op in (LpOp.MINT, LpOp.INCREASE):
            approvals = tuple(
                ApprovalNeed(token, manager, amount, amount)
                for token, amount in ((pool.token0, intent.amount0), (pool.token1, intent.amount1))
                if amount > 0
            )
        staging = tuple(Call(a.token, erc20_approve(a.spender, a.approve_amount)) for a in approvals)
        return Plan(manager, 0, deadline, NfpmBody(call, collect, shape), approvals, None, staging)

    def _posm(
        self,
        intent: LpIntent,
        pool: Pool,
        deadline: int,
        shape: LpShape,
        sqrt_price_x96: int,
        position: PositionState | None,
        permit_nonces: dict[str, int],
    ) -> Plan:
        wallet = intent.wallet
        key = pool.key
        params: list[PosmParam]
        value = 0
        approvals: tuple[ApprovalNeed, ...] = ()
        permit: PermitBatch | None = None
        staging: tuple[Call, ...] = ()
        if intent.op in (LpOp.MINT, LpOp.INCREASE):
            liquidity = liquidity_for_amounts(sqrt_price_x96, sqrt_ratio_at_tick(shape.tick_lower), sqrt_ratio_at_tick(shape.tick_upper), intent.amount0, intent.amount1)
            if liquidity <= 0:
                raise TxError("invalid_intent", "amounts yield zero liquidity at the current price")
            shape = replace(shape, liquidity=liquidity)
            if intent.op is LpOp.MINT:
                params = [PosmMint(key, shape.tick_lower, shape.tick_upper, liquidity, MAX_UINT128, MAX_UINT128, wallet)]
            else:
                params = [PosmIncrease(intent.token_id, liquidity, MAX_UINT128, MAX_UINT128)]
            params.append(PosmSettlePair(pool.token0, pool.token1))
            if pool.token0 == NATIVE:
                params.append(PosmSweep(NATIVE, wallet))
                value = intent.amount0
            details = tuple(
                PermitDetails(token, amount, deadline, permit_nonces[token])
                for token, amount in ((pool.token0, intent.amount0), (pool.token1, intent.amount1))
                if token != NATIVE and amount > 0
            )
            if details:
                permit = PermitBatch(details, POSM, deadline)
                approvals = tuple(ApprovalNeed(d.token, PERMIT2, MAX_UINT256, d.amount) for d in details)
                staging = tuple(
                    call
                    for d in details
                    for call in (Call(d.token, erc20_approve(PERMIT2, MAX_UINT256)), Call(PERMIT2, permit2_approve(d.token, POSM, d.amount, deadline)))
                )
        else:
            assert position is not None
            liquidity = intent.liquidity if intent.op is LpOp.DECREASE else 0
            if intent.op is LpOp.DECREASE and not 0 < liquidity <= position.liquidity:
                raise TxError("invalid_intent", "liquidity must be within the position")
            shape = replace(shape, liquidity=liquidity)
            params = [PosmDecrease(intent.token_id, liquidity, 0, 0), PosmTakePair(pool.token0, pool.token1, wallet)]
        return Plan(POSM, value, deadline, PosmBody(tuple(params), shape), approvals, permit, staging)

    def finalize(self, plan: Plan, bounds: tuple[int, int], sigs: Signatures) -> Plan:
        body = plan.body
        b0, b1 = bounds
        if isinstance(body, NfpmBody):
            call = body.call
            if isinstance(call, (NfpmMint, NfpmIncrease, NfpmDecrease)):
                call = replace(call, amount0_min=b0, amount1_min=b1)
            return replace(plan, body=replace(body, call=call))
        if isinstance(body, PosmBody):
            if plan.permit is not None and sigs.permit is None:
                raise TxError("permit_required", "the quote needs a Permit2 batch signature")
            params = []
            for p in body.params:
                if isinstance(p, (PosmMint, PosmIncrease)):
                    p = replace(p, amount0_max=b0, amount1_max=b1)
                elif isinstance(p, PosmDecrease):
                    p = replace(p, amount0_min=b0, amount1_min=b1)
                params.append(p)
            return replace(plan, body=replace(body, params=tuple(params)), signature=sigs.permit)
        raise TxError("internal", "not an lp plan")

    def amounts(self, shape: LpShape, ledger: Ledger, slippage_bps: int, sqrt_price_x96: int | None) -> LpAmounts:
        pool = shape.pool
        token_id = shape.token_id
        if shape.op is LpOp.MINT:
            minted = [t for m, w, t in ledger.nft_mints if m == shape.manager and w == shape.wallet.lower()]
            if len(minted) != 1:
                raise TxError("unmodeled_fee", f"expected one minted position, saw {len(minted)}")
            token_id = minted[0]
        adding = shape.op in (LpOp.MINT, LpOp.INCREASE)
        if shape.manager == POSM:
            deltas = [d for pid, d in ledger.liquidity_deltas if pid == pool.id]
            if len(deltas) != 1:
                raise TxError("unmodeled_fee", f"expected one ModifyLiquidity log, saw {len(deltas)}")
            liquidity = abs(deltas[0])
            flow0, flow1 = ledger.flow(pool.token0, shape.wallet), ledger.flow(pool.token1, shape.wallet)
            amount0, amount1 = (-flow0, -flow1) if adding else (flow0, flow1)
            fees0 = fees1 = None
            if not adding and sqrt_price_x96 is not None:
                p0, p1 = principal_for_liquidity(sqrt_price_x96, shape.tick_lower, shape.tick_upper, liquidity, round_up=False)
                fees0, fees1 = amount0 - p0, amount1 - p1
        else:
            events = [e for e in ledger.nfpm_events if e[0] == shape.manager and e[2] == token_id]
            if adding:
                inc = [e for e in events if e[1] == TOPIC_NFPM_INCREASE]
                if len(inc) != 1:
                    raise TxError("unmodeled_fee", f"expected one IncreaseLiquidity log, saw {len(inc)}")
                _, _, _, liquidity, amount0, amount1 = inc[0]
                fees0 = fees1 = None
            else:
                dec = [e for e in events if e[1] == TOPIC_NFPM_DECREASE]
                col = [e for e in events if e[1] == TOPIC_NFPM_COLLECT]
                if len(col) != 1 or len(dec) > 1:
                    raise TxError("unmodeled_fee", f"expected one Collect log, saw {len(col)}")
                liquidity, p0, p1 = (dec[0][3], dec[0][4], dec[0][5]) if dec else (0, 0, 0)
                amount0, amount1 = col[0][4], col[0][5]
                fees0, fees1 = amount0 - p0, amount1 - p1
        if amount0 < 0 or amount1 < 0:
            raise TxError("unmodeled_fee", "negative liquidity flow")
        if adding and shape.manager == POSM:
            bound0, bound1 = _bound_up(amount0, slippage_bps), _bound_up(amount1, slippage_bps)
        elif adding:
            bound0, bound1 = min_out_for(amount0, slippage_bps), min_out_for(amount1, slippage_bps)
        elif shape.op is LpOp.COLLECT:
            bound0 = bound1 = 0
        else:
            p0 = amount0 - (fees0 or 0)
            p1 = amount1 - (fees1 or 0)
            bound0, bound1 = min_out_for(p0, slippage_bps), min_out_for(p1, slippage_bps)
        return LpAmounts(liquidity, amount0, amount1, fees0, fees1, token_id, bound0, bound1)


def _bound_up(amount: int, slippage_bps: int) -> int:
    return -(-amount * (10_000 + slippage_bps) // 10_000)


__all__ = [
    "Amounts", "ApprovalNeed", "Call", "Fee", "FeeLeg", "Intent", "Ledger", "LpAmounts", "LpIntent",
    "LpOp", "LpPlanner", "LpShape", "NfpmBody", "Plan", "PositionState", "PosmBody", "Signatures",
    "SwapBody", "SwapIntent", "SwapPlanner", "SwapShape", "TxError", "TxPolicy", "TxRefusal",
    "impact_bps", "liquidity_for_amounts", "min_out_for", "principal_for_liquidity", "swap_amounts",
]
