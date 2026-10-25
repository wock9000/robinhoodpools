# Transaction core, swaps and LP for rhpools: immutable executor candidate

Candidate 2 of arena `tx`. Direction: one minimal, immutable `RhpoolsSwapExecutor`
that pulls the input through Permit2, runs 1–3 pool legs, takes the 75 bps fee and
enforces min-out on chain in a single transaction. LP goes straight to the canonical
managers with no rhpools contract in the path.

## Problem

Units 4–5 need one transaction core (route, quote, simulate-from-user at a pinned
block, prepare unsigned, track receipt) under three constraints that pull against
each other. The server never signs, so every economic guarantee must be carried by
the transaction the user signs. The 75 bps fee must be collected atomically and the
done predicate says the recipient receives *exactly* 75 bps. Robinhood Chain's swap
surface is heterogeneous: the Universal Router carries a non-standard
`minHopPriceX36` ABI, V2/V3 routers only know the Uniswap factories, Pancake and Giga
V3 pools use `pancakeV3SwapCallback`, and Pons pools are V4 pools quoted in native
ETH whose hook charges two floor-divided fees in `afterSwap`. Without a contract, a
75 bps fee is a second transfer the user can drop from the calldata and min-out lives
in router calldata that differs per venue. With a contract, the fee, the min-out, the
deadline, the venue ABIs and the ETH/WETH plumbing collapse into one audited surface
and one calldata shape; the price is an audit and an on-chain dependency rhpools has
never had. Existing code that constrains the shape: `workbench_actions.ActionService`
(pinned-block quote, binding hash, TTL, fresh preflight at prepare), the
stdlib `_ROUTES`/`do_POST` handler with `_same_origin`, `workbench.js` raw EIP-1193
wallet code, and the `pools` table (protocol, address, token0/1, fee_ppm,
tick_spacing, hook, factory) as the only route-discovery input.

## Usage (caller's view)

### Operator quickstart

```
# one-time: deploy and pin the executor (fee recipient + 75 bps baked in, no owner)
forge script script/Deploy.s.sol --rpc-url $RH_RPC --broadcast --sig 'run(address,uint16)' $FEE_RECIPIENT 75
# systemd unit gains:
Environment=RHP_EXECUTOR=0x…           # address printed by the script
Environment=RHP_EXECUTOR_CODEHASH=0x…  # keccak(runtime code); server refuses trade quotes on mismatch
```

Trade and LP features are gated by the entitlement service from unit 0 (`Feature.trade`,
`Feature.lp`); anonymous requests to every existing route are untouched.

### Server: quoting a Pons buy (handler → service)

```python
# lp_server.Handler.do_POST, new branch (session + entitlement resolved by unit 1/0 code)
wallet = self.runtime.gate.require(self._session(), Feature.trade)   # raises Forbidden, fail closed
quote = self.runtime.trade.quote(TradeRequest.parse(payload, wallet))  # ValueError → 400
self._json(200, quote.to_public())

# inside TradeService.quote
pin = self.core.pin()                                    # latest block number+hash, one read
route = self.routes.best(pin, request.token_in, request.token_out, request.amount_in)
q = self.core.quote_swap(pin, route, request.amount_in)  # one eth_call to executor.quoteExactIn
sim = self.core.simulate_from(pin, wallet, self.builder.swap_tx(route, q, request))  # eth_call + estimateGas from wallet
return self.core.store(SwapQuote(..., fee=q.fee_breakdown, sim=sim, expires=pin.at + 30))
```

### Browser: the trade ticket

```js
import { Wallet } from "./wallet.js";        // EIP-1193 wrapper, chain 4663 pinned
import { Ticket } from "./tx_ticket.js";
const ticket = new Ticket(document.getElementById("trade-ticket"), Wallet.shared());
ticket.open({ side: "buy", token: "0x92d5…c604" });     // renders idle → quoting → quoted (countdown)
// user clicks CONFIRM: ticket.prepare() → POST /api/trade/prepare → [optional Permit2 typed-data sign]
// → eth_sendTransaction → pending → GET /api/tx/receipt?hash= until confirmed|failed
```

### Foundry: fork test call site

```solidity
Order memory o = Order({legs: legs, tokenIn: NATIVE, tokenOut: MEME, amountIn: 0.05 ether,
    minAmountOut: quoted.netOut * 9900 / 10000, feeOnOutput: false, deadline: block.timestamp + 60, permit: empty});
uint256 net = executor.swapExactIn{value: 0.05 ether}(o);
assertEq(feeRecipient.balance - before, 0.05 ether * 75 / 10_000);
```

## Shape

### Data structures

```
Venue        = V2 | V3 | V4
Leg          = venue, pool (V2 pair | V3 pool | 0 for V4), token_in, token_out,
               fee, tick_spacing, hooks            (fee/tick/hooks only meaningful for V4)
Route        = legs[1..3], token_in, token_out     (adjacent legs join on the same token, or ETH⇄WETH)
FeeBreakdown = pool_fee_bps, hook_fee_bps, creator_tax_bps, rhpools_fee_bps=75,
               rhpools_fee_raw, price_impact_bps     (impact = residual after fees vs pinned spot)
SwapQuote    = id, pin(block, hash, at), wallet, route, amount_in, gross_out, net_out,
               min_out, fee: FeeBreakdown, approvals: ApprovalPlan, sim: Simulation, expires_at
ApprovalPlan = [ErcApprove(token, spender, amount) | Permit2Permit(typed_data)]   # exact amounts, expiry=deadline
PreparedTx   = from, to, data, value, chainId, gas, quote_id, typed_data?          # what the wallet signs
Receipt      = hash, status ∈ {pending, confirmed, failed}, block, decoded: Swapped|LpDelta|None
LpIntent     = manager ∈ {UNI_V3, PANCAKE_V3, GIGA_V3, V4_POSM}, op ∈ {mint, increase, decrease, collect},
               pool_key|pool, tick_lower, tick_upper, amounts, liquidity_bps, slippage_bps, use_native
```

`Route` and `Leg` are the domain form of the executor's `Order` struct; the wire
(ABI) form is produced in one place (`tx_abi.encode_order`) and never exposed.
The quote store is per-process (like `ActionService._quotes`), keyed by id, holding
a sha256 binding; nothing about trades touches the market SQLite writer. Dominant
access patterns: quote by id (dict), route candidates by token pair (one indexed read
of `pools` per side), receipt by hash (one RPC). No caches beyond the 30 s quote TTL.

### The contract

`contracts/src/RhpoolsSwapExecutor.sol`, Solidity 0.8.26, cancun, no dependencies
except interface stubs. No owner, no upgrade, no rescue, no allowlist of tokens or
pools, no configurable venue. Pinned constants: `POOL_MANAGER`, `WETH`, `PERMIT2`,
`V2_FACTORY` (Uniswap V2 only). Immutables: `feeRecipient`, `feeBps` (≤ `MAX_FEE_BPS = 100`).

```solidity
struct Leg { uint8 venue; address pool; address tokenIn; address tokenOut; uint24 fee; int24 tickSpacing; address hooks; }
struct Permit { IPermit2.PermitSingle single; bytes signature; }   // signature.length == 0 → skip permit
struct Order {
    Leg[] legs; address tokenIn; address tokenOut;     // NATIVE = address(0) allowed at either end only
    uint256 amountIn; uint256 minAmountOut; bool feeOnOutput; uint256 deadline; Permit permit;
}
event Swapped(address indexed user, address indexed tokenIn, address indexed tokenOut,
              uint256 amountIn, uint256 grossOut, uint256 feeAmount, uint256 netOut);
error Expired(); error BadOrder(); error BadLeg(uint8 index); error Slippage(uint256 net, uint256 min);
error Residue(); error TransferTaxed(); error Reentrant(); error FeeTransferFailed(); error NotPool();

/// @notice Exact-in swap. msg.value == amountIn iff tokenIn == NATIVE. Recipient is always msg.sender.
/// Fee = amountIn*feeBps/1e4 taken before leg 0 (feeOnOutput=false) or grossOut*feeBps/1e4 after the
/// last leg (feeOnOutput=true). Reverts unless netOut >= minAmountOut and the input is fully consumed.
function swapExactIn(Order calldata o) external payable returns (uint256 netOut) { /* TODO see flow */ }

/// @notice Same legs, no funds: every leg runs and reverts with its output (QuoterV2 pattern).
/// Callable by anyone from any address; used by the server for quotes and by the UI for refresh.
function quoteExactIn(Order calldata o) external returns (uint256 grossOut, uint256 feeAmount, uint256 netOut, uint256[] memory legOut) {}

function unlockCallback(bytes calldata data) external returns (bytes memory);          // msg.sender == POOL_MANAGER
function uniswapV3SwapCallback(int256 a0, int256 a1, bytes calldata) external;          // msg.sender == tstore'd pool
function pancakeV3SwapCallback(int256 a0, int256 a1, bytes calldata) external;          // same handler
receive() external payable;                                                              // only WETH or POOL_MANAGER
```

Execution flow (`swapExactIn`), pseudocode:

```
tlock();  require deadline > now; 1 <= legs <= 3; amountIn,minOut > 0; msg.value == (tokenIn==NATIVE ? amountIn : 0)
check continuity: legs[0].tokenIn == tokenIn; legs[i].tokenOut ~ legs[i+1].tokenIn (equal or ETH⇄WETH); last.tokenOut == tokenOut
if tokenIn != NATIVE: if permit.signature != "" try PERMIT2.permit(msg.sender, single, sig) {} catch {}   // idempotent
                      PERMIT2.transferFrom(msg.sender, this, uint160(amountIn), tokenIn); require Δbalance == amountIn (TransferTaxed)
held = amountIn; if !feeOnOutput: fee = held*feeBps/1e4; held -= fee; pay(tokenIn, feeRecipient, fee)
for leg in legs: held = _leg(leg, held, quote=false)      // wraps/unwraps at the boundary, measures Δbalance
gross = held; if feeOnOutput: fee = gross*feeBps/1e4; net = gross - fee; pay(tokenOut, feeRecipient, fee) else net = gross
require net >= minOut (Slippage); pay(tokenOut, msg.sender, net)
require balance(tokenIn) == before && (tokenIn==NATIVE ? this.balance == before : true) (Residue)
emit Swapped; tunlock()
```

Leg semantics: **V2** — `pool.factory() == V2_FACTORY`, transfer input to the pair,
`amountOut = getAmountOut(reserves, 997/1000)`, `pair.swap(...)`. **V3** — set
`tstore(EXPECTED_POOL, pool)`, `pool.swap(this, zeroForOne, +amountIn, limit, "")`,
pay owed token in the callback, clear. Callback names differ per fork; both route to
one `_pay`. **V4** — `POOL_MANAGER.unlock(abi.encode(leg, amountIn, quote))`; in
`unlockCallback`: `swap(key, {zeroForOne, -amountIn, limit}, "")` returns the
*hook-adjusted* delta; settle the negative side (native `settle{value}` or
`sync+transfer+settle`), `take` the positive side to `this`. Quote mode reverts with
the delta instead of settling. Hook data is always empty; any hook is accepted — the
delta the PoolManager returns is what the user gets, and min-out guards it.

What is deliberately not in the contract: token/pool allowlists (server's job; the
user signs their own calldata and can only hurt themselves), exact-out, split routes,
router calls, ownership. Interface depth: two external entry points hide five venue
ABIs, two callback conventions, native/WETH plumbing, Permit2 pulls, fee math and
residue checks; the caller supplies a `Order` and reads one event.

### Python

```python
# src/rhpools/tx_core.py — pinning, simulation, quote store, receipts (shared by trade and lp)
@dataclass(frozen=True)
class Pin: block: int; hash: str; at: int
@dataclass(frozen=True)
class Simulation: success: bool; gas: int | None; revert: str | None      # revert decoded via tx_abi.decode_error
@dataclass(frozen=True)
class PreparedTx: sender: str; to: str; data: str; value: int; gas: int; quote_id: str; typed_data: dict | None

class TxCore:
    """Block-pinned RPC facade over the read-only Nitro node; owns the quote store and receipt decoding."""
    def __init__(self, rpc, executor: ExecutorPin, ttl_s: int = 30) -> None: raise NotImplementedError
    def pin(self) -> Pin: raise NotImplementedError                                  # eth_getBlockByNumber(latest)
    def verify_executor(self) -> None: raise NotImplementedError                     # eth_getCode hash == RHP_EXECUTOR_CODEHASH, else trades disabled
    def call(self, to: str, data: bytes, pin: Pin, sender: str | None = None, value: int = 0) -> bytes: raise NotImplementedError
    def simulate_from(self, pin: Pin, sender: str, tx: PreparedTx) -> Simulation: raise NotImplementedError   # eth_call + eth_estimateGas at pin.block
    def store(self, quote: "SwapQuote | LpQuote") -> str: raise NotImplementedError   # id, sha256 binding, TTL eviction (ActionService pattern)
    def load(self, quote_id: str, wallet: str) -> "SwapQuote | LpQuote": raise NotImplementedError  # expired/mismatch → ValueError
    def receipt(self, tx_hash: str) -> Receipt: raise NotImplementedError            # eth_getTransactionReceipt on local node; decodes Swapped / NFPM / POSM events

# src/rhpools/tx_abi.py — pure encoding, no I/O
def encode_order(route: Route, amount_in: int, min_out: int, fee_on_output: bool, deadline: int, permit: Permit2Permit | None) -> bytes: raise NotImplementedError
def decode_quote_result(data: bytes) -> tuple[int, int, int, list[int]]: raise NotImplementedError
def decode_error(data: bytes) -> str: raise NotImplementedError        # executor/PoolManager/NFPM custom errors → human text ("Slippage", "Expired", hook revert, …)
def encode_nfpm_mint(p: NfpmMint) -> bytes: ...; def encode_nfpm_multicall(calls: list[bytes]) -> bytes: ...
def encode_posm_modify(actions: bytes, params: list[bytes], deadline: int) -> bytes: raise NotImplementedError
def permit2_typed_data(owner: str, token: str, spender: str, amount: int, expiry: int, nonce: int) -> dict: raise NotImplementedError

# src/rhpools/tx_routes.py — candidate routes from the pools table (read-only reader), no RPC
class RouteFinder:
    def __init__(self, store) -> None: raise NotImplementedError
    def candidates(self, token_in: str, token_out: str) -> list[Route]: raise NotImplementedError
        # direct pools on both tokens (V2 Uniswap factory only, V3 all three factories, V4 any hook);
        # two-leg via WETH/ETH or USDG; ETH⇄WETH joins are free legs. Pons pools: hooks == PONS_HOOK, quoted vs NATIVE.
    def pons_policy(self, core: TxCore, pin: Pin, pool_id: bytes) -> tuple[int, int]: raise NotImplementedError  # launches(): word7 creator, word10 hook

# src/rhpools/tx_trade.py — unit 4
class TradeService:
    def __init__(self, core: TxCore, routes: RouteFinder, executor: ExecutorPin) -> None: raise NotImplementedError
    def quote(self, req: TradeRequest) -> SwapQuote: raise NotImplementedError
        # best(candidates) by executor.quoteExactIn at pin; FeeBreakdown from pool fee_ppm, launches(), feeBps; impact vs spot
        # approvals: NATIVE in → none; ERC20 in → [ERC20 approve(token→PERMIT2, amount) if allowance < amount] + Permit2 typed data
        # sim: eth_call swapExactIn from wallet at pin with state override {wallet.balance: +value} for native; ERC20 sim only after approvals exist
    def prepare(self, quote_id: str, wallet: str, step: str) -> PreparedTx: raise NotImplementedError
        # re-pin, block-hash unchanged or price within slippage (re-quote), fresh simulate_from; returns approve|swap tx + typed data

# src/rhpools/tx_position.py — unit 5, direct to canonical managers
class PositionService:
    def __init__(self, core: TxCore, managers: Mapping[str, ManagerInfo]) -> None: raise NotImplementedError
    def quote(self, intent: LpIntent, wallet: str) -> LpQuote: raise NotImplementedError
        # V3: NFPM mint/increaseLiquidity/decreaseLiquidity/collect(+multicall unwrapWETH9/sweepToken/refundETH for native)
        # V4: POSM.modifyLiquidities([MINT_POSITION|INCREASE|DECREASE, SETTLE_PAIR|TAKE_PAIR, SWEEP?], deadline); token pulls via Permit2
        # hook gate: refuse if hooks has BEFORE/AFTER_ADD_LIQUIDITY flags or the pinned eth_call from wallet reverts inside the hook;
        #            warn "pool fee 0 + hook: LP earns no swap fee" for Pons pools
        # mins: amount0Min/amount1Min from slippage_bps against pinned sqrtPrice; deadline = pin.at + 120
    def prepare(self, quote_id: str, wallet: str, step: str) -> PreparedTx: raise NotImplementedError

# gate seam (unit 0 contract, consumed here): runtime.gate.require(session, Feature) -> wallet | raises Forbidden
```

Business logic (fee math, min-out, impact, ABI encoding, hook-flag decoding) is pure
and unit-tested without RPC; `TxCore` is the only I/O owner, per boundary-discipline.
Wire dicts from the RPC are parsed into `Pin`/`Simulation`/`Receipt` at the edge.

### HTTP surface (all POST unless noted; same-origin + session cookie + entitlement; JSON)

```
/api/trade/quote    trade  → SwapQuote.to_public()            /api/trade/prepare     trade  → PreparedTx
/api/lp-tx/quote    lp     → LpQuote.to_public()              /api/lp-tx/prepare     lp     → PreparedTx
/api/tx/receipt     GET, trade|lp → Receipt                    /api/tx/config          GET, anon → {executor, codehash, fee_bps, verified_source_url}
```

Added to `_ROUTES`-adjacent POST set, the tunnel path regex, and `docs/PUBLIC_API.md`.
No loopback restriction (unlike workbench prepare) because these routes are gated by
session + entitlement and never sign. `MAX_BODY` stays 16 KiB (an `Order` with a
Permit2 signature is < 2 KiB). CSP unchanged: no new external scripts.

### Browser

```
static/wallet.js      Wallet.shared(): connect(), ensureChain(), account, signTypedData(v4), sendTransaction(tx), on(accountsChanged|chainChanged)
                      (extracted from workbench.js by copy, workbench.js untouched so the anonymous /pool surface is byte-identical)
static/tx_ticket.js   Ticket: states idle → quoting → quoted(countdown 30 s) → expired | preparing → awaiting-signature
                      → pending → confirmed | failed | rejected;  fee breakdown rows: pool fee, hook fee, creator tax, rhpools 0.75%,
                      price impact (colored: <1% dim, 1–3% yellow, >3% red), min received, expires-in; approval sub-steps shown inline
static/lp_panel.js    Panel per manager: op tabs MINT/INCREASE/DECREASE/COLLECT; range from ticks; mins; hook-blocked = disabled tab + reason
lp_terminal.html/css  new #trade-ticket and #lp-panel sections behind the header gate state from unit 1; theme tokens only, .badge reuse
```

### Module map

```
contracts/                      foundry project (foundry.toml, src/RhpoolsSwapExecutor.sol, script/Deploy.s.sol, test/*.t.sol)
src/rhpools/tx_core.py          TxCore, Pin, Simulation, PreparedTx, Receipt, ExecutorPin
src/rhpools/tx_abi.py           pure ABI encode/decode for executor, NFPM, POSM, Permit2 typed data, error decoding
src/rhpools/tx_routes.py        RouteFinder over the pools table reader; Pons policy read
src/rhpools/tx_trade.py         TradeService (quote/prepare)          src/rhpools/tx_position.py  PositionService (quote/prepare)
src/rhpools/lp_server.py        4 POST + 2 GET routes, gate seam, tunnel/CSP untouched
static/wallet.js, tx_ticket.js, lp_panel.js, lp_terminal.{html,css,js} (sections + nav entries)
deploy/robinhoodpools.service   RHP_EXECUTOR, RHP_EXECUTOR_CODEHASH        deploy/robinhoodpools-tunnel.json  path regex
tests/test_tx_*.py, tests/fork/  (anvil fork), tests/e2e/ (browser)
```

Tracing a swap: `tx_ticket.js → lp_server → tx_trade → tx_core/tx_abi` (three files
server-side); the contract is the fourth and final hop.

### Invariants and where they live

- Server never signs/broadcasts: no signing code exists; `PreparedTx` has no signature field (type).
- Every swap carries min-out and deadline: `Order` fields are required and the contract reverts on zero/expired (type + chain).
- Fee is exactly `feeBps` of the quote-side amount: immutable in bytecode; server recomputes with the same floor for display (chain; test asserts recipient delta).
- Approvals are exact-amount with expiry: `ApprovalPlan` only holds `Permit2Permit(amount = amountIn, expiration = deadline)` and exact `ERC20 approve`; unlimited approvals cannot be expressed (type). V3 NFPM has no Permit2 → exact ERC-20 approve per operation.
- Targets pinned with code-hash checks: `ExecutorPin(address, codehash)` verified at startup and every 5 min; mismatch → `/api/trade/*` returns 503 `executor_unverified` (fail closed). Manager addresses come from `lp_market_protocols.MANAGER_INFO`.
- Anonymous routes unchanged: no existing route or asset is edited except `lp_terminal.*` and `lp_server.py` route additions; golden compare in CI.
- Gate never touches the market writer: trade/lp state is the in-memory quote store; route discovery uses the existing reader.
- Quotes are idempotent to re-prepare: `prepare` twice returns the same calldata for the same pin; a broadcast retry with a consumed Permit2 nonce still succeeds because the executor's permit call is try/catch and the allowance check governs.

### Deployment and verification plan

1. `forge build` with pinned `solc 0.8.26`, `evm_version = cancun`, optimizer 200, `bytecode_hash = none`, `cbor_metadata = false` so the runtime code hash is reproducible from source.
2. `script/Deploy.s.sol` deploys through the Arachnid CREATE2 factory (`0x4e59…956c`, present on 4663) with salt `keccak("rhpools-executor-v1")`, then sends 1 wei to `feeRecipient` and reverts the run if that transfer fails (recipient must accept ETH). Prints address and `keccak(runtime code)`.
3. Verify on Blockscout (`forge verify-contract --verifier blockscout --verifier-url https://robinhoodchain.blockscout.com/api/`); if the explorer's Cloudflare challenge blocks the API, publish the standard-JSON input and creation tx hash in `contracts/DEPLOYMENT.md` so anyone can reproduce the code hash.
4. Pin `RHP_EXECUTOR` and `RHP_EXECUTOR_CODEHASH` in the systemd unit; `/api/tx/config` exposes both plus the source URL, and the ticket shows the executor address next to CONFIRM.
5. Fee recipient change or fee change = new deployment + host config change; the old executor keeps working for anyone who pinned it. There is nothing to migrate.

### Test and verification harness

- **Foundry fork tests** (`contracts/test`, `RH_RPC=http://127.0.0.1:8547 forge test`): Pons buy with ETH (recipient +75 bps of `msg.value`, net == quote, two-floor hook model), Pons sell (fee on output ETH), V3 exact-in on Uniswap/Pancake/Giga pools (both callback names), V2 pair, two-leg USDG→WETH(V3)→ETH→Pons, min-out revert, deadline revert, `Residue` on fee-on-transfer input, reentrancy via malicious token, callback from a non-pool (`NotPool`), `receive` from a stranger, fee recipient reverting, Permit2 path with a consumed nonce. Seed: `forge-poc/` in this directory already proves the V4-direct and V3-direct legs on the live fork.
- **Server tests** (pytest, `FakeRPC` pattern from `test_workbench_actions.py`): pure encoding round-trips against `cast abi-encode` fixtures, fee/impact math, hook-flag gate, quote TTL/binding, entitlement fail-closed (403 without session, without feature), executor code-hash mismatch → 503.
- **Anvil fork** (`tests/fork/conftest.py`: `anvil --fork-url 127.0.0.1:8547 --port <free>`; deploy executor + stand-in ERC-20 gate token; `anvil_setBalance`): end-to-end quote → prepare → `eth_sendTransaction` from an anvil account → receipt decode; balance reconciliation to the wei for V3 NFPM and V4 POSM mint/increase/decrease/collect (done predicate 6); Pons and V3 token buy/sell (predicate 5).
- **Browser e2e**: Playwright (dev dependency only) against the served terminal with an injected test provider (`page.addInitScript`) that forwards `eth_requestAccounts`, `eth_sendTransaction`, `eth_signTypedData_v4` to the anvil fork's unlocked account; asserts ticket states (quoted → expired countdown, pending → confirmed, slippage revert → failed with decoded reason) and the fee rows.
- **Golden compare**: record anonymous responses of every existing route on the snapshot, diff after the change.

### Chain facts relied on

| Fact | Status |
|---|---|
| chainId 4663; Permit2, UR, POSM, PoolManager, NFPM×3, V2/V3 routers, Pons hook, WETH all have code | verified, `cast codesize` |
| POSM.permit2() == canonical Permit2; POSM.poolManager() == PoolManager | verified, `cast call` |
| NFPM factories: Uniswap 0x1f7d, Pancake 0x0bfb, Giga 0xece6; V2 router factory 0x8bce, WETH 0x0bd7 | verified |
| Pancake and Giga V3 pools call `pancakeV3SwapCallback` (0x23a69e75); Uniswap pools `uniswapV3SwapCallback` | verified: selector present in pool code and fork swap executed on all three |
| Pons `launches(bytes32)` returns 13 words, word7 = creator bps (190), word10 = hook bps (100) on pool `0x22bd…1c6c` | verified |
| Pons hook permissions: beforeInitialize, afterSwap, afterSwapReturnDelta only — adds are **not** hook-blocked | verified, `getHookPermissions()` |
| Direct `PoolManager.unlock → swap → settle/take` from a non-router contract works on a Pons pool; returned delta is hook-adjusted; net == core − ⌊core·100/1e4⌋ − ⌊core·190/1e4⌋ | verified, `forge-poc` fork test |
| USDG has 6 decimals, WETH 18 | verified |
| PoolManager, POSM, hook selectors (unlock, swap, settle, take, sync, modifyLiquidities, …) present | verified |
| v4 `Actions` byte values (MINT 0x02, INCREASE 0x00, DECREASE 0x01, SETTLE_PAIR 0x0d, TAKE_PAIR 0x11, SWEEP 0x14) | verified against v4-periphery main; assumed to match the deployed POSM build |
| Transient storage available (ArbOS ≥ 30; live v4 swaps depend on TSTORE) | verified by inference; `arbOSVersion()` = 116 |
| Nitro `eth_call` accepts state overrides | verified |
| CREATE2 factory 0x4e59…956c present; Multicall3 present | verified |
| Blockscout verification API reachable from scripts | unverified (Cloudflare challenge on curl); fallback in plan |
| Uniswap V2 factory pairs charge 0.3 %; other two V2 factories' fees | assumed for 0x8bce (router uses 997); others unsupported |

## Synthesis decision

Filled in by arena.

## Tradeoffs accepted

- We accept a smart-contract audit (est. 300–350 lines, one external auditor week; the surface is the two callbacks, `receive`, native-ETH accounting, Permit2 pull, residue check) in exchange for on-chain fee enforcement, one calldata shape for every venue, and quotes that are the execution path itself.
- We accept immutability of `feeRecipient`/`feeBps` (redeploy to change) in exchange for a contract with no admin key to compromise and nothing for the audit to reason about after deployment.
- We accept V2 support limited to the Uniswap V2 factory in exchange for not encoding per-fork fee constants on chain.
- We accept two wallet prompts on ERC-20 sells (typed-data permit + transaction, plus one exact `approve(token→Permit2)` per trade by default) in exchange for exact, expiring allowances; a "trust Permit2 once (unlimited)" toggle is an explicit user choice, off by default.
- We accept that dust stuck in the executor (fee-on-transfer outputs, rounding) is unrecoverable, like Apollo's executor, in exchange for no rescue path.
- We accept that Pons LP adds are allowed but warned (fee-0 pool) rather than refused, because the hook does not block them; "hook-blocked" is decided by flags and simulation, not by hook identity.

## Alternatives considered

- **No contract; server-built router calldata plus a separate fee transfer (a UR `PAY_PORTION`-style split).** Exposes every router ABI (including the non-standard `minHopPriceX36`) to the server and the user, cannot make the fee atomic without UR-specific commands, and needs a different calldata shape per venue. Hides nothing; the caller learns the implementation.
- **Executor that wraps the Universal Router (Apollo's shape).** One fewer callback to audit but inherits an ABI rhpools cannot verify from source, cannot reach Pancake/Giga V3 pools, and still needs native-ETH plumbing. Depth is lower: the router's quirks leak through the executor's interface.
- **Quoting off chain (relay-listener `/v1/quote` or local pool math).** Adds a Rust service dependency and a second implementation of the Pons two-floor math that must agree with the hook; the on-chain quoter is the same code path the user executes and needs no funds.
- **Fee as a capped per-call parameter (Apollo).** Lets the server lower the fee later without redeploying, but a user can set it to zero by editing calldata, so the chain no longer enforces it. Rebates are deferred anyway and can be settled off chain.

## Open questions and risks

- Is a redeploy acceptable as the only way to change the fee recipient, or must the recipient be rotatable (which reintroduces an owner key)?
- Should ERC-20 sells default to exact `approve(token→Permit2)` per trade (two transactions) or to the industry-standard unlimited Permit2 approval once? The invariant says exact; the UX cost is real.
- Who audits, and does launch wait for the report? Until then the ticket could show "unaudited" and cap `amountIn` server-side.
- Pons pools accept LP adds but pay LPs nothing (fee 0). Refuse, warn, or hide LP for hooked pools entirely?
- Do we support the two non-Uniswap V2 factories at all (their fees are unknown to us)?
- Blockscout verification may need manual submission; is a reproducible code hash plus published standard JSON sufficient for the "verified source" link?

## Next implementation step

Turn `forge-poc/` into `contracts/`: write `RhpoolsSwapExecutor.sol` against the interface above with the Pons buy/sell and three-factory V3 fork tests as the first failing tests, since every server type is derived from `Order` and `Swapped`.
