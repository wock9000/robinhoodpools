# rhpools transaction core, candidate 3: relay-listener as the quote engine

Units 4 and 5 of TOKEN_PLAN.md. Direction: the relay-listener's pinned-state route
quoter is the only source of "how much do I get"; rhpools verifies its evidence,
adds the 75 bps fee, encodes one Universal Router call, simulates it at the same
pin from the user's address, hands the wallet an unsigned transaction, and
reconciles the receipt. LP never touches the listener.

## Problem

rhpools is a stdlib server that must never sign, hold keys, or broadcast; the
only existing transaction path builds unsigned V3 burn/collect calldata behind
`--enable-transaction-prepare` and a loopback check (`workbench_actions.py`,
`lp_server.py:641-686`). Trading needs route discovery and an amount-out oracle
across V2, V3 and V4 (Pons-hooked) pools, a 75 bps fee that lands atomically with
the swap, exact-amount Permit2 approvals, min-out and deadline on every
transaction, and a fork-provable receipt story. The relay-listener already owns
a route graph, quotes every candidate at one canonical pin, models the PonsV2
hook's two fee floors, encodes the Robinhood Universal Router's non-standard ABI,
and exposes a loopback `/v1/execution-quote` with a keccak evidence hash that
apollo already verifies. Reusing it removes the largest body of new code from
rhpools. The cost is a live dependency on a process rhpools does not own, whose
route work (PRs #3/#4) is unmerged. Both facts shape the design below.

## Grounding facts

Every chain fact this design relies on, and whether it was verified (RPC,
fork transaction, source read) or assumed, is tabulated in RATIONALE.md
"Chain facts relied on". Load-bearing verified ones: USDG has 6 decimals; the
Robinhood UR takes 6-field V2/V3 swap inputs and accepts `PAY_PORTION`/`SWEEP`,
and a fork transaction paid exactly `gross*75//10000` to the fee recipient; the
Pons hook's `launches(poolId)` returns creator tax at word 7 and hook fee at
word 10 and its permission bits do not gate liquidity; local Nitro supports
block-hash-pinned `eth_call`, state overrides and `eth_simulateV1`; the live
listener at 127.0.0.1:4331 quotes V3 routes but refuses Pons pools.

## Usage (caller's view)

### Terminal user

1. Sign in (unit 1). With `trade` entitlement the section nav gains `TRADE`; with
   `lp` it gains `LP`. Anonymous pages are byte-identical.
2. TRADE ticket: pick token (search or click a tape row), side BUY/SELL, quote
   asset USDG or WETH, amount, slippage (default 50 bps). The ticket shows
   `QUOTED` with: route legs, expected out, guaranteed min out, fee breakdown
   (pool fees per leg, Pons hook 100 bps + creator N bps when a Pons leg is
   present, rhpools 75 bps in the quote asset), price impact vs pinned mid, pin
   block, and a 90 s expiry countdown. States: `IDLE`, `QUOTING`, `QUOTED`,
   `EXPIRED`, `NO ROUTE`, `ENGINE OFFLINE`, `SIGN APPROVAL`, `SIGN PERMIT`,
   `CONFIRM IN WALLET`, `PENDING`, `CONFIRMED (fee paid X)`, `FAILED (reason)`.
3. LP panel: choose protocol/pool (from POOLS or the inspector), action
   MINT/INCREASE/DECREASE/COLLECT, range and amounts. Shows mins, hook check
   (`HOOK GATES ADDS`, `HOOK GATES REMOVES`, `0 BPS LP FEE, HOOK KEEPS SWAP FEES`),
   then the same sign/pending/confirmed states.

### HTTP (all under `/api/tx/`, POST bodies JSON, session cookie from unit 1)

```
GET  /api/tx/status                     -> {"quote_engine": {"reachable": true, "api_version": "relay-simulation.v1", "checked_at": ...}, "fee_bps": 75, "fee_recipient": "0x..", "quote_assets": [...]}
POST /api/tx/quote     (feature trade)  {"owner","side":"buy|sell","token","quote_asset","amount_raw","slippage_bps"}      -> Ticket
POST /api/tx/lp        (feature lp)     {"owner","protocol","pool","action","tick_lower","tick_upper","amount0_raw","amount1_raw","token_id"?,"liquidity_bps"?,"slippage_bps"} -> Ticket
POST /api/tx/prepare   (ticket owner)   {"ticket_id","owner","step_id","signature"?}                                       -> {"step": Step with unsigned tx, "simulation": SimResult}
GET  /api/tx/receipt?ticket_id&step_id&hash                                                                                 -> Receipt
```

Errors keep the existing `{"error": str}` shape; 403 for gate refusals, 409 for
expired/reorged tickets, 422 for `no_route`, 503 `{"error":"quote engine unavailable","retry":5}`.

### Server call sites

```python
# lp_server.py, inside do_POST after _same_origin(); loopback is NOT required for /api/tx/*
wallet = self.runtime.gate.require("trade", self)          # raises GateRefused -> 403
ticket = self.runtime.tx.quote_swap(SwapRequest.parse(payload, wallet))
self._json(200, ticket.public())

# prepare: same for both swap and LP tickets
step, sim = self.runtime.tx.prepare(payload["ticket_id"], wallet, payload["step_id"], payload.get("signature"))
```

```python
# tx_core.py, quote_swap, the whole trade flow in one method
pin_quote = self.engine.quote(req.input_asset, req.output_asset, req.engine_amount_in, req.slippage_bps, deadline)
pons = self.chain.pons_policy(pin_quote.route, pin_quote.pin)            # launches() per Pons leg, at the pin
plan = SwapPlan.build(req, pin_quote, pons, self.fee)                     # pure: fee split, min_out, legs
calldata = universal_router.encode_swap(plan)                             # pure
sim = self.chain.simulate(plan.simulation_bundle(), pin_quote.pin)        # eth_simulateV1 from the user's address
return self.tickets.open(Ticket.swap(plan, calldata, sim, wallet))
```

### Browser call sites (`static/tx_ticket.js`, vanilla)

```js
const ticket = await api.post("/api/tx/quote", form.request());        // renders QUOTED or an error state
for (const step of ticket.steps) {
  const prepared = await api.post("/api/tx/prepare", { ticket_id: ticket.id, owner, step_id: step.id, signature });
  if (step.kind === "permit-sign") signature = await wallet.signTypedData(prepared.step.typed_data); // eth_signTypedData_v4, no tx
  else hash = await wallet.sendTransaction(prepared.step.tx);           // eth_sendTransaction; wallet sets fees
  await receipts.follow(ticket.id, step.id, hash);                       // polls /api/tx/receipt, drives PENDING/CONFIRMED/FAILED
}
```

## Shape

### Data structures (`tx_types.py`, frozen dataclasses)

```python
# frozen dataclasses unless noted; every value below is immutable once a ticket is opened
class Pin(NamedTuple): number: int; hash: str            # one canonical block; every read/simulation in a ticket names it
class Asset: address: str; symbol: str; decimals: int    # native ETH = 0x000…0; never a ticket quote asset
class PoolKey: currency0: str; currency1: str; fee: int; tick_spacing: int; hooks: str   # pool_id = keccak(abi.encode(key))

class V2Leg(NamedTuple):  tokens: tuple[str, ...]; pools: tuple[str, ...]
class V3Leg(NamedTuple):  tokens: tuple[str, ...]; fees: tuple[int, ...]; pools: tuple[str, ...]
class V4Leg(NamedTuple):  key: PoolKey; zero_for_one: bool
class Wrap(NamedTuple): ...
class Unwrap(NamedTuple): ...
Leg = V2Leg | V3Leg | V4Leg | Wrap | Unwrap

class Route:
    legs: tuple[Leg, ...]; input: Asset; output: Asset
    def pons_legs(self) -> tuple[V4Leg, ...]: ...          # legs whose key.hooks == PONS_HOOK

class EngineQuote:              # parsed, verified listener evidence; the wire shape never leaves tx_quote_engine.py
    pin: Pin; route: Route; amount_in: int; quoted_out: int; engine_min_out: int
    deadline: int; evidence_hash: str; engine: str            # "relay-listener/relay-simulation.v1"

class PonsFees:                 # launches(poolId) at the pin, one per Pons leg
    pool_id: str; hook_fee_bps: int; creator_tax_bps: int
    hook_fee_raw: int; creator_tax_raw: int                    # display estimates recovered from net output, ±1 wei

class FeeBreakdown:
    rhpools_bps: int; rhpools_raw: int; rhpools_asset: Asset  # always the quote asset (USDG/WETH)
    pool_fee_bps: tuple[int, ...]; pons: tuple[PonsFees, ...]; price_impact_bps: int

class SwapPlan:                 # everything the encoder and the ticket need; pure value
    side: Literal["buy", "sell"]; wallet: str; route: Route
    amount_in: int              # leaves the wallet (buy: quote asset incl. fee; sell: token)
    swap_in: int                # buy: amount_in - fee; sell: amount_in
    quoted_out: int; min_out: int          # net of every fee; enforced on chain
    fee: FeeBreakdown; deadline: int; pin: Pin; evidence_hash: str
    permit: PermitSingle        # exact amount_in, expiration = deadline, spender = UR

class PermitSingle:             # Permit2 AllowanceTransfer; the wallet signs, UR/PositionManager consume
    token: str; amount: int; expiration: int; nonce: int; spender: str; sig_deadline: int
    def typed_data(self) -> dict: ...                          # EIP-712 JSON for eth_signTypedData_v4

class UnsignedTx: sender: str; to: str; data: str; value: int; gas: int; chain_id: int = 4663   # no signature field exists
class SimResult:  ok: bool; gas: int | None; revert: RevertReason | None; out_raw: int | None; fee_raw: int | None  # from simulated Transfer logs
class RevertReason:
    code: Literal["slippage", "deadline", "hook_blocked_add", "hook_blocked_remove", "insufficient_balance", "allowance", "unknown"]
    detail: str; raw: str

class Step:
    id: str                     # "approve-<token>", "permit", "execute" | "approve-0", "approve-1", "modify"
    kind: Literal["approve", "permit-sign", "send"]
    tx: UnsignedTx | None; typed_data: dict | None; simulation: SimResult

class Ticket:
    id: str; wallet: str; kind: Literal["swap", "lp"]; plan: SwapPlan | LpPlan
    steps: tuple[Step, ...]; expires_at: int; binding_hash: str
    def public(self) -> dict: ...                              # browser JSON; no wire types

class HookCheck:                # decoded from the low 14 bits of the hook address + key.fee
    hooks: str; flags: frozenset[str]; gates_adds: bool; gates_removes: bool; lp_fee_bps: int

class LpPlan:
    protocol: Literal["uniswap-v3", "pancake-v3", "giga-v3", "v4"]; action: Literal["mint", "increase", "decrease", "collect"]
    wallet: str; pin: Pin; pool: str | PoolKey; manager: str; token_id: int | None
    tick_lower: int; tick_upper: int; liquidity: int
    amount0: int; amount1: int; amount0_bound: int; amount1_bound: int   # max on add, min on remove
    hook: HookCheck | None; deadline: int

class Receipt:
    hash: str; status: Literal["pending", "confirmed", "failed", "replaced", "stale"]
    block: Pin | None; gas_used: int | None
    out_raw: int | None; fee_raw: int | None; fee_reconciled: bool | None; revert: RevertReason | None
```

Invariants encoded in these types: a `SwapPlan` cannot exist without a `Pin`, a
`min_out`, a `deadline` and an `evidence_hash` (no unpinned or unbounded swap can
be encoded); `UnsignedTx` has no signature field; the fee asset is the quote
asset by construction, so the fee recipient never holds a meme token and every
fee is an ERC-20 `Transfer` log the receipt can reconcile exactly; native ETH
never appears as a ticket asset (Pons legs wrap/unwrap inside the router).

### Fee and min-out arithmetic (pure, `tx_plan.py`)

- Buy: `fee = amount_in * 75 // 10000`, paid by `PERMIT2_TRANSFER_FROM(quote, fee_recipient, fee)`
  before the swap; the swap spends `amount_in - fee` (this is the `amount_in_raw`
  sent to the listener). `min_out = engine_min_out` and rhpools re-derives
  `quoted_out * (10000 - slippage) // 10000` and refuses a mismatch, as apollo does.
- Sell: the route ends at `ADDRESS_THIS`; `PAY_PORTION(quote, fee_recipient, 75)`
  then `SWEEP(quote, MSG_SENDER, min_out)` with `min_out = m - m*75//10000`,
  `m = engine_min_out`. Fee is exactly 75 bps of realized gross output.
- Pons display: for a Pons leg with net output `n` and policy `(h, c)`,
  `gross ≈ n * 10000 // (10000 - h - c)`; `hook_fee_raw = gross*h//10000`,
  `creator_tax_raw = gross*c//10000` (the hook applies two separate floors,
  `amm.rs:54-74`). Labelled as estimates; the on-chain min-out is what binds.
- Price impact: mid price at the pin per leg (V2 reserves, V3 `slot0`, V4
  `StateView.getSlot0`), multiplied along the route; `impact = 1 - gross_out /
  mid_out`. Above 300 bps the ticket shows `HIGH IMPACT`; above 1000 bps the
  confirm button needs an explicit second click.

### Universal Router encoding (`tx_universal_router.py`, pure functions)

Commands per side, one `execute(bytes,bytes[],uint256)`:

```
buy  : PERMIT2_PERMIT(permit, sig)                       # exact amount_in, expiration = deadline
       PERMIT2_TRANSFER_FROM(quote, fee_recipient, fee)  # atomic fee, exact by construction
       <legs>(first leg payerIsUser=true, amountIn=swap_in; later legs CONTRACT_BALANCE; last leg recipient=MSG_SENDER, amountOutMin=min_out)
sell : PERMIT2_PERMIT(permit, sig)
       <legs>(first leg payerIsUser=true; last leg recipient=ADDRESS_THIS, amountOutMin=0)
       PAY_PORTION(quote, fee_recipient, 75)
       SWEEP(quote, MSG_SENDER, min_out)
legs : V2Leg  -> 0x08 (recipient, amountIn, amountOutMin, address[] path, payerIsUser, uint256[] minHopPriceX36=[])
       V3Leg  -> 0x00 (recipient, amountIn, amountOutMin, bytes path(token,fee3,token…), payerIsUser, uint256[] = [])
       Unwrap -> 0x0c (ADDRESS_THIS, 0)      Wrap -> 0x0b (ADDRESS_THIS, CONTRACT_BALANCE)
       V4Leg  -> 0x10 actions [SETTLE(currencyIn, amount|CONTRACT_BALANCE, payerIsUser), SWAP_EXACT_IN_SINGLE(key, zeroForOne, OPEN_DELTA|amount, amountOutMin, minHopPriceX36=0, hookData=""), TAKE(currencyOut, recipient, OPEN_DELTA)]
```

A Pons sell (meme→ETH pool→USDG) is `V4Leg, Wrap, V3Leg, PAY_PORTION, SWEEP`; a
Pons buy from USDG is `PERMIT2_TRANSFER_FROM(fee), V3Leg(→ADDRESS_THIS), Unwrap, V4Leg(TAKE→MSG_SENDER)`.
When the first leg is `Unwrap` (WETH quote asset straight into a native-quoted
V4 pool) a `PERMIT2_TRANSFER_FROM(WETH, ADDRESS_THIS, swap_in)` precedes it,
since wrap/unwrap legs have no payer of their own.
Signatures:

```python
def encode_swap(plan: SwapPlan, permit_signature: bytes | None) -> bytes: raise NotImplementedError
def v3_path(tokens: Sequence[str], fees: Sequence[int]) -> bytes: raise NotImplementedError
def v4_swap_input(leg: V4Leg, amount_in: int, min_out: int, recipient: str, payer_is_user: bool) -> bytes: raise NotImplementedError
def decode_revert(data: bytes) -> RevertReason: raise NotImplementedError   # V4TooLittleReceived, "Too little received", TransactionDeadlinePassed, InsufficientToken, Permit2 AllowanceExpired/InsufficientAllowance, hook selectors
```

`permit_signature=None` produces the quote-time simulation variant: the
`PERMIT2_PERMIT` command is dropped and the simulation bundle prepends a
`Permit2.approve(token, UR, amount, expiration)` call from the user, so the
economic path is simulated before the user signs anything. The prepare-time
simulation uses the exact final calldata with the signature.

### Quote engine adapter (`tx_quote_engine.py`)

```python
class QuoteEngineDown(RuntimeError): ...   # -> 503
class NoRoute(ValueError): ...             # -> 422
class EvidenceRejected(RuntimeError): ...  # -> 502, logged with the offending field

class ListenerQuoteEngine:
    """Loopback client for relay-simulation.v1 /v1/execution-quote. Verifies before parsing."""
    def __init__(self, base_url: str, timeout_s: float = 8.0, breaker: CircuitBreaker = ...) -> None: ...
    def health(self) -> EngineHealth: raise NotImplementedError          # GET /health, cached 5 s
    def quote(self, input: Asset, output: Asset, amount_in: int, slippage_bps: int, deadline: int) -> EngineQuote:
        # 1. POST; 503/connection error -> QuoteEngineDown (one retry after 500 ms covers the collector's 3 s restart window only if a second probe succeeds; otherwise fail fast)
        # 2. exact key set, api_version == "relay-simulation.v1", authority/source/chain_id/state_backend/state_health literal checks, deadline echo, hash regexes
        # 3. keccak(canonical_json(evidence)) == evidence_hash, same canonicalisation as execution_http.rs:108-126 (sorted keys, no whitespace)
        # 4. route -> Route: v2/v3/v4 single-protocol today; every pool.identity_verified_at_block <= pin.number; tokens[0]==input, tokens[-1]==output; pool token pairs match hops
        # 5. every pool identity re-derived at the pin: V2/V3 pool.token0/token1/fee via eth_call, V4 pool_id == keccak(key). A route we cannot re-derive is EvidenceRejected, never executed
        raise NotImplementedError
```

Step 5 is what makes the listener advisory rather than trusted: rhpools only
ever encodes pools whose identity it recomputed at the pin against allowlisted
factories/PoolManager. The listener contributes the search and the amount; the
chain contributes the identity.

`route.legs[]` adapter: when the listener starts returning multi-leg routes (the
change set in RATIONALE.md "Coupling assessment") the same parser yields
multi-leg `Route`s; the encoder already handles them, so no rhpools change
beyond the parser.

### Core (`tx_core.py`)

```python
class TxCore:
    """Shared by swap and LP: pinned reads, simulation, tickets, prepare, receipts. Owns no keys."""
    def __init__(self, rpc: RpcClient, engine: ListenerQuoteEngine, gate_db: GateStore, fee: FeeConfig, allowlist: Allowlist) -> None: ...
    def quote_swap(self, req: SwapRequest) -> Ticket: raise NotImplementedError
    def plan_lp(self, req: LpRequest) -> Ticket: raise NotImplementedError
    def prepare(self, ticket_id: str, wallet: str, step_id: str, signature: str | None) -> tuple[Step, SimResult]:
        # ticket exists, not expired, wallet matches, binding hash intact (pattern from workbench_actions._verify_record)
        # pin still canonical (eth_getBlockByNumber(pin.number).hash == pin.hash) else 409 "rebuild the quote"
        # allowlist code hashes unchanged at latest
        # re-simulate the exact tx (with signature for permit-bearing steps) at latest via eth_simulateV1 from the wallet; gas = sim.gas * 12 // 10
        raise NotImplementedError
    def receipt(self, ticket_id: str, step_id: str, tx_hash: str) -> Receipt: raise NotImplementedError
    def status(self) -> dict: raise NotImplementedError                     # engine health, fee config, allowlist state

class ChainReads:                # every method takes a Pin; nothing here reads "latest" implicitly
    def pons_policy(self, route: Route, pin: Pin) -> tuple[PonsFees, ...]: raise NotImplementedError
    def hook_check(self, key: PoolKey) -> HookCheck: raise NotImplementedError          # pure on address bits + key.fee
    def mid_prices(self, route: Route, pin: Pin) -> tuple[Fraction, ...]: raise NotImplementedError   # Multicall3 aggregate3 at pin
    def permit2_state(self, owner: str, token: str, spender: str, pin: Pin) -> tuple[int, int, int]: raise NotImplementedError  # amount, expiration, nonce
    def erc20_allowance(self, owner: str, token: str, spender: str, pin: Pin) -> int: raise NotImplementedError
    def simulate(self, calls: Sequence[UnsignedTx], pin: Pin | Literal["latest"]) -> SimResult: raise NotImplementedError
        # eth_simulateV1 blockStateCalls=[{"calls": calls}] with EIP-1898 pin; decode Transfer logs to user and fee recipient;
        # a failed call yields decode_revert(error.data)
    def canonical_receipt(self, tx_hash: str) -> Receipt: raise NotImplementedError
        # receipt + header at receipt.blockNumber must carry receipt.blockHash; status 0 -> debug_traceCall replay at block-1 for the revert selector

class TicketStore:               # in-memory dict + lock, TTL = deadline; a ticket is immutable after open()
    def open(self, ticket: Ticket) -> Ticket: raise NotImplementedError
    def get(self, ticket_id: str, wallet: str) -> Ticket: raise NotImplementedError    # 409 on expiry
```

Tickets live in process memory like today's `_quotes`; they are worthless after
90 s and a restart simply forces a re-quote. Nothing about a ticket is written to
either SQLite. Audit of what was quoted/prepared goes to the gate DB as append-
only rows `(ticket_id, wallet, kind, evidence_hash, pin, fee_raw, created)`;
the market writer is never touched.

### LP (`tx_lp.py`)

```python
V3_MANAGERS = {"uniswap-v3": ("0x7399…de03", UNISWAP_V3_FACTORY), "pancake-v3": ("0x46a1…4364", PANCAKE_V3_FACTORY), "giga-v3": ("0xa79f…f641", EXTENDED_V3_FACTORY)}

def plan_v3(req: LpRequest, pool: PoolState, position: PositionState | None, pin: Pin) -> LpPlan: raise NotImplementedError
    # liquidity from amounts via sqrtPrice/tick math (reuse workbench_actions helpers); bounds = amounts*(1±slippage); deadline
def plan_v4(req: LpRequest, key: PoolKey, slot0: Slot0, position: V4Position | None, pin: Pin) -> LpPlan: raise NotImplementedError
def steps_v3(plan: LpPlan, allowances: dict[str, int]) -> tuple[Step, ...]: raise NotImplementedError
    # approve-<token> (ERC20.approve(manager, exact amount_bound)) only where allowance < needed; then mint/increaseLiquidity/decreaseLiquidity/collect calldata
def steps_v4(plan: LpPlan, permit2: dict[str, tuple[int, int, int]]) -> tuple[Step, ...]: raise NotImplementedError
    # approve-0/approve-1 (ERC20.approve(Permit2, exact)) where short; one "permit" step (PermitBatch typed data, spender = PositionManager, expiration = deadline);
    # "modify" = PositionManager.multicall([permitBatch(owner, batch, sig), modifyLiquidities(actions, deadline)])
    # actions: mint  -> MINT_POSITION(key, tl, tu, liquidity, amount0Max, amount1Max, owner, "") + SETTLE_PAIR (+ SWEEP(ETH, owner) when currency0 is native; value = amount0Max)
    #          incr  -> INCREASE_LIQUIDITY(tokenId, liquidity, max0, max1, "") + SETTLE_PAIR
    #          decr  -> DECREASE_LIQUIDITY(tokenId, liquidity, min0, min1, "") + TAKE_PAIR(c0, c1, owner)
    #          collect -> DECREASE_LIQUIDITY(tokenId, 0, 0, 0, "") + TAKE_PAIR
def refuse_if_hook_blocked(plan: LpPlan, sim: SimResult) -> None: raise NotImplementedError
    # adds: hook.gates_adds and not sim.ok -> ActionError(code "hook_blocked_add", revert detail)
    # adds on pools where hook.lp_fee_bps == 0 and hooks == PONS_HOOK -> ActionError("pons_pool_no_lp_fee") by default policy (owner toggle, see open questions)
    # mints on pools whose hook gates removes -> the ticket carries a "HOOK GATES REMOVES" warning that requires a second confirm
```

V3 collect/decrease reconcile "to the wei" because the receipt decoder reads
`Collect`/`DecreaseLiquidity` events from the manager and `Transfer` logs to the
owner; V4 reads `ModifyLiquidity` on PoolManager plus `Transfer` logs.

### Allowlist (`tx_allowlist.py`)

Pinned `{address: code_hash}` for UR, Permit2, PositionManager, PoolManager, the
three NFPMs, Pons hook, Multicall3, StateView, V2/V3 factories. Checked once at
startup (a mismatch disables `trade`/`lp` with a loud status) and again in
`prepare` at latest. Every `UnsignedTx.to` must be in the allowlist or be an
ERC-20 the ticket names (approve steps).

### Gating and server wiring

`lp_server.py` gains one prefix branch: `/api/tx/*` routes are POST-or-GET,
require `_same_origin()`, do **not** require loopback, and call
`runtime.gate.require(feature, handler)` before touching `runtime.tx`. Existing
routes and their handlers are untouched; `_ROUTES`, `_ASSETS`, CSP and
`do_OPTIONS` gain entries only. Tunnel allowlist gains `^/api/tx/` and the two
new static files. New CLI/env: `--tx-fee-recipient`, `--tx-fee-bps` (default 75,
owner-only), `--quote-engine-url` (default `http://127.0.0.1:4331`, must be
loopback). Without `--tx-fee-recipient` the trade feature is disabled and
`/api/tx/status` says why. `/api/tx/*` responses are credentialed, so they carry
no `Access-Control-Allow-Origin` header and `Cache-Control: no-store`; the
public `ACAO: *` JSON path is not reused for them.

### Browser (`static/tx_wallet.js`, `static/tx_ticket.js`)

```js
// tx_wallet.js: the only file that touches window.ethereum for the terminal (workbench.js keeps its own copy for /pool)
export async function connect(): Promise<{account, chainId}>            // eth_requestAccounts + wallet_switchEthereumChain 0x1237
export async function signTypedData(account, typedData): Promise<string> // eth_signTypedData_v4
export async function sendTransaction(tx): Promise<string>               // eth_sendTransaction with {from,to,data,value,gas,chainId}; never fee params
export function onAccountsChanged(cb), onChainChanged(cb)

// tx_ticket.js: state machine + rendering in the terminal theme; no wallet calls except through tx_wallet.js
const TicketState = Object.freeze({ IDLE, QUOTING, QUOTED, EXPIRED, NO_ROUTE, ENGINE_OFFLINE, SIGN_APPROVAL, SIGN_PERMIT, CONFIRM, PENDING, CONFIRMED, FAILED });
function renderFeeBreakdown(fee)      // rows: pool fee per leg, PONS HOOK 100 bps, CREATOR n bps, RHPOOLS 75 bps, IMPACT; uses .badge.good/.warn/.bad
function startExpiryCountdown(ticket)  // QUOTED -> EXPIRED at expires_at; disables confirm 5 s before
function followReceipt(ticketId, stepId, hash) // GET /api/tx/receipt every 2 s up to 20 min; maps to PENDING/CONFIRMED/FAILED with decoded reason
```

Sections `#trade-section` and `#lp-section` are appended to `lp_terminal.html`
with `hidden` and revealed by the entitlement payload from unit 1; the strip
shows `ENGINE OFFLINE` in `--rhp-red` when `/api/tx/status.quote_engine.reachable`
is false and the user holds `trade`.

### Engine coupling, in one paragraph

The live `relay-simulation.v1` service serves single-protocol V2/V3 routes and
hookless-input V4 routes today, and refuses Pons pools; multi-leg Pons routes
require the listener change set described in RATIONALE.md "Coupling
assessment" (legs in the hashed evidence, a `--quote-only` mode, a
rhpools-owned unit on 127.0.0.1:4332). Until it lands, Pons tokens show
`NO ROUTE (engine cannot quote this pool yet)` and done predicate 5 is blocked;
V3 tokens and all of LP are not.

What happens when the engine is down (collector restart, apollo redeploy, RPC
race failure, host reboot ordering):

- `/api/tx/quote` returns 503 with `retry: 5`; the ticket shows `ENGINE OFFLINE`
  with a countdown and re-quotes automatically once `/api/tx/status` flips. No
  other quoter is consulted and no stale quote is served: a quote older than its
  90 s deadline is unusable by construction.
- Open tickets keep working: `prepare` and `receipt` use only rhpools' RPC. A
  user who quoted before the blip can still sign, send and see confirmation.
- LP is unaffected; it never calls the engine.
- Health is a first-class status: `/api/tx/status` and the strip badge, plus the
  existing healthcheck timer gains an `/api/tx/status` probe that alerts but does
  not restart anything it doesn't own.

### Module map

```
src/rhpools/
  tx_types.py            frozen dataclasses above; no I/O
  tx_plan.py             SwapPlan.build, fee/min-out/impact/Pons arithmetic; pure
  tx_universal_router.py UR command encoders, V4 actions, Permit2 typed data, revert decoding; pure
  tx_lp.py               V3/V4 LP plans, steps, hook policy; pure except ChainReads inputs
  tx_quote_engine.py     ListenerQuoteEngine (requests, loopback only), evidence verification, Route parsing
  tx_allowlist.py        pinned addresses + code hashes, startup and prepare checks
  tx_core.py             TxCore, ChainReads, TicketStore, receipts, audit rows -> gate DB
  lp_server.py           + /api/tx/* branch, CLI flags, assets, CSP entries (existing routes untouched)
  static/tx_wallet.js    EIP-1193 only
  static/tx_ticket.js    trade ticket + LP panel state machines and rendering
  static/lp_terminal.{html,css}  + two hidden sections and ticket styles on theme tokens
tests/
  test_tx_universal_router.py   golden calldata (fork-run vectors), decode_revert table
  test_tx_plan.py               fee floors, min-out, Pons split, impact
  test_tx_quote_engine.py       fixture responses: hash mismatch, extra key, pin > identity block, hop mismatch -> rejected
  test_tx_lp.py                 liquidity math, action bytes, hook flags (Pons address -> no add gate)
  test_tx_server.py             gate refusal, byte-identical anonymous routes (golden compare), 503 when engine down
  fork/                         anvil harness (below)
```

Trace of a buy reads three files: `lp_server.py` (auth + dispatch) →
`tx_core.py` (flow) → `tx_plan.py`/`tx_universal_router.py` (pure). The engine
adapter sits beside, not below.

## Test and verification harness

Fork harness (`tests/fork/conftest.py`, pytest marker `fork`, skipped without
`RHP_FORK_RPC`): spawns `anvil --fork-url http://127.0.0.1:8547 --no-mining
--chain-id 4663` on a free port, funds USDG by impersonating the WETH/USDG V3
pool (worked in this session's probe), deploys the stand-in ERC-20 for the gate,
and points a `ListenerQuoteEngine` fake at recorded live responses with `pin`
rewritten to the fork head and the evidence hash recomputed (the hash is an
integrity check over canonical JSON, not a signature), so the verification code
path runs unchanged. With the listener change set
in place the harness can instead point a `--quote-only` instance at the anvil
URL for a fully live route.

Fork proofs mapped to done predicates 5 and 6:

- V3 token buy and sell: fee recipient `Transfer` == `amount_in*75//10000` (buy)
  and == `gross*75//10000` (sell); user receives ≥ `min_out`; expired ticket →
  409; deadline in the past → `TransactionDeadlinePassed` decoded as `deadline`;
  price moved past slippage (harness performs a large swap before mining) →
  `Too little received` / `V4TooLittleReceived` decoded as `slippage`.
- Pons token buy and sell once the engine exposes legs: same assertions plus
  hook/creator fee estimate within 1 wei of `gross - net` from the swap logs.
- LP: Uniswap/Pancake/Giga NFPM mint → increase → decrease → collect and V4
  PositionManager mint → increase → decrease → collect; balances before/after
  equal the plan's amounts to the wei; adds to a pool whose hook has the
  beforeAddLiquidity bit and reverts are refused with `hook_blocked_add`
  (harness `anvil_setCode`s a reverting hook at a flagged address); Pons pool
  add refused by policy.
- Allowlist drift: `anvil_setCode` on UR → `prepare` refuses.

Browser e2e (`tests/e2e/ticket.mjs`, playwright-core already on this host via
apollo, headless Chromium): injects a `window.ethereum` shim via
`addInitScript` that proxies `eth_requestAccounts`, `eth_chainId`,
`eth_signTypedData_v4` and `eth_sendTransaction` to the anvil fork's unlocked
dev account, so no key material exists in the test. Walks: sign in → TRADE →
quote → countdown visible → approve → permit signature → confirm → PENDING →
CONFIRMED with fee line; then lets the quote expire and asserts `EXPIRED`
disables confirm; then kills the fake engine and asserts `ENGINE OFFLINE`.
Anonymous golden compare: fetch every existing route before and after with no
cookie and diff bodies and headers.

## Synthesis decision

Left for arena.

## Alternatives considered

In RATIONALE.md "Alternatives considered": rhpools-native quoter, custom
executor contract, fixed-amount sell fee, degraded quoting during outages,
unlimited Permit2 pre-approval. Each was judged on what it exposes to callers
versus what it hides.

## Tradeoffs accepted

- We accept a hard runtime dependency on a process owned by apollo, in exchange
  for not writing route discovery, a pool index, V4 quoting and Pons fee
  modelling in Python. The dependency is loopback, verified per response, and
  visible in status; the mitigation path (quote-only unit owned by rhpools) is
  specified.
- We accept that Pons trading ships only after the listener change set, in
  exchange for a single quote provenance and no second quoter to maintain.
- We accept two extra wallet interactions per trade (exact ERC-20 approve to
  Permit2 when short, then one typed-data signature) in exchange for the owner's
  exact-amount, expiring approvals; no unlimited allowances are ever requested.
- We accept USDG/WETH as the only quote assets (no native ETH tickets) in
  exchange for every fee being an ERC-20 `Transfer` the receipt reconciles
  exactly and a fee recipient that never holds memes or raw ETH from trades.
- We accept that sell fees are 75 bps of the router's post-swap balance, which
  includes any pre-existing dust in the router (observed 0 WETH dust at the fork
  block), in exchange for a percentage of realized rather than quoted output.
- We accept the listener's canonical-RPC pin (`state_health: VERIFIED` means
  "checked receipts or canonical anchor", README:201-204) as the quote pin; the
  binding guarantee is the on-chain min-out, not the quote.
- We accept in-memory tickets lost on restart; a ticket is 90 s of state.

## Open questions and risks

- Will the listener owners accept the change set (legs in evidence, `--quote-only`,
  a rhpools-owned unit)? If not, does the owner prefer shipping V3-only trading
  now and Pons later, or switching to the native-quoter candidate?
- Pons pools do not gate adds on chain; they are fee-0 pools whose swap fees go
  to the hook and creator. Should the LP panel refuse adds to them (default in
  this sketch) or warn and allow?
- Exact-amount ERC-20 approvals to Permit2 cost one extra transaction per trade
  when the allowance is short. Is an owner-approved "approve Permit2 once" toggle
  acceptable, or is exact-per-trade non-negotiable?
- Is a 90 s quote/ticket deadline right for a launch-day meme ticket, or should
  the owner be able to set it (listener caps at 300 s)?
- The fee recipient is an owner-configured EOA/contract; if it is a contract,
  `PAY_PORTION` of WETH/USDG is a plain ERC-20 transfer, fine; confirm it is not
  a contract that rejects tokens.
- `eth_simulateV1` on Nitro answered a single call; its multi-call and
  block-hash-pinned behaviour is assumed standard until the harness confirms it.
  Fallback is sequential `eth_call` with state overrides for the Permit2
  allowance slot.
- Apollo's collector currently reports `ready: false` with 10k gaps while the
  quote endpoint answers; rhpools should key health on `/health` of 4331/4332,
  not on collector readiness. Confirm with the listener owners that quote
  service availability is decoupled from collector readiness by design.

## Next implementation step

Write `tx_universal_router.py` with `encode_swap` and the golden test built from
this session's fork transaction (commands `0x000604`, fee `2785034688572698` of
`371337958476359751`), then `ListenerQuoteEngine.quote` against a recorded live
response, so the two boundaries with verified facts are pinned before the flow.
