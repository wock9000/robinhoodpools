# Rationale, candidate 3 (listener as quote engine)

Companion to DESIGN.md. Holds the coupling assessment, the alternatives that
were considered and rejected, and the full table of chain facts with how each
was checked.

## Why this direction can be the best shape

The listener already owns the three hardest parts of unit 4 for Robinhood Chain:
a pool index that recovers complete V4 PoolKeys from the PositionManager and
Initialize logs, per-candidate quoting at one canonical pin with pool identity
evidence, and the only known correct encoding of the Robinhood Universal
Router's non-standard ABI (6-field V2/V3 inputs, 6-field V4 exact-in-single with
`minHopPriceX36`), plus the PonsV2 two-floor fee model. Its `/v1/execution-quote`
contract is already consumed and verified by a production caller (apollo's
`robinhood-direct-provider.ts`), so the verification code and the failure modes
are known. rhpools keeps the parts that must be rhpools': the fee, the recipients,
min-out, deadline, the allowlist, the simulation from the user's address, the
ticket, the wallet interaction and the receipt. That split leaves the listener
advisory: it can only make rhpools refuse or route worse, never move funds.

## Coupling assessment

Today rhpools can consume the live `relay-simulation.v1` unchanged for
single-protocol routes: USDG/WETH ↔ any token with a canonical Uniswap V2/V3
pool. It cannot get Pons quotes from the live service (refusal verified, see the
facts table below),
because the deployed binary lacks PR #3's cross-venue `Route` and its V4 support
is single-pool with the input token required to be the pool's own currency.

Required listener change set (a listener PR, not rhpools code):

1. Port `execution_http.rs` onto the PR #3/#4 lineage (or merge PR #3/#4 into
   apollo's vendored copy) so `/v1/execution-quote` returns
   `route.legs[]` (`{kind: v2|v3|v4|wrap|unwrap, tokens, fees, pools|pool_key}`)
   inside the hashed evidence, alongside the existing single-protocol fields.
2. A `--quote-only` mode that runs the pool index, discovery and the HTTP
   service with `ROBINHOOD_RPC_URLS` only, without Yellowstone/Relay credentials.
3. A rhpools-owned unit `rhpools-quote.service` running a pinned binary on
   `127.0.0.1:4332` from the local Nitro RPC, `Restart=always`, independent of
   apollo deploys.

Until 1–3 land, `--quote-engine-url` points at apollo's 4331 and the ticket
refuses Pons tokens with `NO ROUTE (engine cannot quote this pool yet)`. Done
predicate 5 (Pons buy/sell on the fork) is therefore blocked on the listener
work in this direction; V3 tokens and the whole LP unit are not.

Operational coupling, stated plainly:

- The quote service is a side-car thread of the collector child process that
  apollo's node collector spawns; it restarts whenever apollo redeploys or the
  collector's stdout/Postgres path fails (`Restart=always`, 3 s). Port 4331
  disappears for a few seconds each time; rhpools' breaker turns that into
  `ENGINE OFFLINE` and re-quotes, never into a stale quote.
- The binary cannot start without Yellowstone and Relay credentials that belong
  to apollo; rhpools cannot run its own instance from the shipped code today.
  That is why item 2 of the change set exists.
- PR #3 (+2,000 lines) and PR #4 (57 files) are open research code on a
  personal fork. The deployed apollo copy diverged the other way (it added the
  HTTP service and FOMO observers). Someone has to reconcile the two lineages;
  rhpools is a consumer of that work, not its owner, and this design gives
  rhpools no leverage over its timeline other than the V3-only interim.
- The listener's `state_health: VERIFIED` is a canonical-RPC head pin, not a
  state proof (README:201-204); the same local Nitro node backs both rhpools
  and the listener, so a node fault affects both simultaneously and
  independently of the listener process.
- Collector readiness (`/health` on 4330 said `ready: false`, 10k gaps) is
  unrelated to quote availability (4331 answered in ~1 s); health must be keyed
  on 4331/4332 only.

Threat framing:

Verification of every response (exact keys, literals, evidence hash, pin ≤
identity block, pool identity re-derived on chain) means a compromised or buggy
listener can at worst make rhpools refuse to quote or quote a worse-than-market
route through real allowlisted pools; it cannot redirect funds, because `to`,
recipients, fee recipient, min-out and deadline are all set by rhpools and the
route's pools are re-derived from allowlisted factories at the pin.

## Alternatives considered

- **rhpools-native quoter** (own pool index + V2/V3/V4 quoter calls). Deepest
  ownership, no external process; but it re-implements the listener's router,
  identity verification and Pons modelling in Python inside a server that must
  not take the market writer, and it forks the route logic that apollo already
  operates. Rejected for this candidate; it is the natural fallback if the
  listener change set is refused.
- **Custom rhpools executor contract** (apollo's `ApolloRobinhoodExecutor` shape,
  fee inside the contract). Gives an exact fee on both sides and one approval
  target, but adds a deployed contract to audit, a code-hash to pin, and a
  second approval surface; the UR already provides `PERMIT2_TRANSFER_FROM` /
  `PAY_PORTION` and the fork proved the split is exact. Rejected.
- **Fee via `TRANSFER` of a fixed quoted amount on sells.** Deterministic but
  wrong when realized output differs from the quote; `PAY_PORTION` charges the
  realized amount. Rejected.
- **Degraded quoting when the engine is down** (single-pool quote through the
  deployed V3/V4 quoter for tokens opened from a pool page). Hides an outage
  behind a different quote provenance and needs the quoter code we chose not to
  write. Rejected; the ticket says `ENGINE OFFLINE`.
- **Per-trade Permit2 `PermitSingle` only, with a one-time unlimited ERC-20
  approve to Permit2.** Fewer clicks, but violates the owner's exact-amount rule.
  Rejected; offered as an owner question in DESIGN.md "Open questions".

## Chain facts relied on

All RPC checks ran read-only against `http://127.0.0.1:8547` or a throwaway
anvil fork of it (port 18611, killed) on 2026-09-27.

| Fact | Status |
|---|---|
| chainId 4663; USDG decimals **6** (rhpools' DECISIONS note saying 18 is wrong) | verified, `cast chain-id`, `decimals()` |
| Code present at Permit2 `0x0000…78ba3`, UR `0x8876…0904`, PositionManager `0x58da…4fa7`, PoolManager `0x8366…0951`, NFPMs Uniswap `0x7399…de03` / Pancake `0x46a1…4364` / Giga `0xa79f…f641`, Pons hook `0xe5e7…e044`, V4 Quoter `0x8dc1…8f94`, V3 Quoter `0x33e8…a9e7`, StateView `0xf333…673b`; code hashes recorded in `tx_allowlist.py` | verified, `eth_getCode` + keccak |
| PositionManager `permit2()` = canonical Permit2, `poolManager()` = PoolManager; UR `poolManager()` = PoolManager; each NFPM's `factory()` = its pinned factory | verified, `cast call` |
| Pons `launches(bytes32)` selector `0xad091230`, 13 words; word0=1 registered, word7 creatorTaxBps (0xbe=190), word10 hookFeeBps (0x64=100) on pool `0x22bd…1c6c` (ETH/0x92d5…c604, fee 0, spacing 200) | verified, `cast call` |
| Pons hook permissions: address bits and `getHookPermissions()` both say **beforeInitialize, afterSwap, afterSwapReturnsDelta only**. The hook does not gate add/remove liquidity | verified. Consequence: "hook-locked liquidity" is a policy fact (fee-0 pool, launch liquidity owned by the hook), not an on-chain block |
| UR accepts `PAY_PORTION` (0x06) and `SWEEP` (0x04); unknown command reverts `InvalidCommandType(uint256)` (`0xd76a1e9e`), expired deadline reverts `0x5bf6f916` | verified, `eth_call` |
| UR `V3_SWAP_EXACT_IN` input is **6 fields** `(address recipient, uint256 amountIn, uint256 amountOutMin, bytes path, bool payerIsUser, uint256[] minHopPriceX36)`; the standard 5-field input reverts `SliceOutOfBounds()` (`0x3b99b53d`) | verified on fork |
| Atomic fee: `execute(0x00 0x06 0x04, [V3 swap→ADDRESS_THIS, PAY_PORTION(WETH, fee, 75), SWEEP(WETH, MSG_SENDER, 0)])` sent on the fork: status 1, gas 160,541, fee recipient received exactly `gross*75//10000` (2785034688572698 of 371337958476359751 wei) | verified on fork |
| V4 `ExactInputSingleParams` on the Robinhood UR carries an extra `uint256` (minHopPriceX36) before `hookData`; V4 action bytes SETTLE 0x0b, SWAP_EXACT_IN_SINGLE 0x06, TAKE 0x0e; `CONTRACT_BALANCE`, `OPEN_DELTA`, `MSG_SENDER`=0x1, `ADDRESS_THIS`=0x2 | from listener `replay.rs:46-57,1447-1489` and its fork smoke (README:297-302); not re-executed by me |
| Local Nitro supports `eth_call` with EIP-1898 `{blockHash, requireCanonical}`, state overrides, `debug_traceCall` callTracer and `eth_simulateV1` (returns `returnData`, `logs`, `gasUsed`) | verified, curl |
| Listener quote service live at `127.0.0.1:4331` (`/health`: `simulation_enabled: true`, `exact_replay_enabled: false`), inside `apollo-relay-listener-collector.service` (node collector spawns the Rust binary; `Restart=always`, `RestartSec=3`) | verified, curl + unit file |
| Live listener quotes USDG→WETH (v3, fee 500 pool, evidence hash present, ~1 s); **refuses** USDG→Pons meme and ETH(0x0)→Pons meme with "no canonical execution route with verified pool identity found" | verified, POST `/v1/execution-quote` |
| The deployed binary (`apollo-copy-runtime/current/workers/relay-trade-listener`) has `execution_http.rs` but **no** `PonsV2`/`hook_fee` code and only single-protocol routes (`execution_route_supported`: V2 ≤3 tokens via WETH, V3 fees {100,500,3000,10000}, V4 single pool with fee 0/spacing 200/Pons hook). The wock worktree (PR #3/#4, `fix/relay-evidence-audit-20260927`) has the multi-leg `Route`, Pons fee floors and UR encoding but **no** HTTP service. The binary cannot run without `YELLOWSTONE_GRPC_ENDPOINT` and a Relay API key (`main.rs:740,759`) | verified by grep/read |
| Permit2 `PermitSingle` typed data, UR `PERMIT2_PERMIT` 0x0a / `PERMIT2_TRANSFER_FROM` 0x02 / `V4_SWAP` 0x10 / `WRAP_ETH` 0x0b / `UNWRAP_WETH` 0x0c; PositionManager actions MINT_POSITION 0x02, INCREASE 0x00, DECREASE 0x01, SETTLE_PAIR 0x0d, TAKE_PAIR 0x11, CLOSE_CURRENCY 0x12, SWEEP 0x14; `Permit2Forwarder.permitBatch` on PositionManager; V3 NFPM `mint/increaseLiquidity/decreaseLiquidity/collect` ABI identical across Uniswap/Pancake/Giga forks | assumed from Uniswap sources (numbering consistent with the three action bytes the listener verified); the fork harness pins them |
