# RATIONALE — candidate 1 (UniversalRouter‑only, no new contracts)

## Why this shape

One target contract for every swap (`UR.execute`), one approval model (Permit2), one fee
mechanism (`PAY_PORTION` on the quote‑currency leg), one slippage bound in one place
(`SWEEP`/`UNWRAP` `amountMin` or the last swap's `amountOutMinimum`), one accounting source
(logs of the same bytes the user signs). The whole Robinhood‑specific surface (six‑field V4
struct, trailing `uint256[] minHopPriceX36` on V2/V3 inputs) is confined to `tx_chain.py` and
pinned by golden bytes that executed on the fork. The core is generic over a `Planner`, so the
LP unit reuses simulate/store/prepare/receipt unchanged and only contributes calldata and a
log interpreter.

## Alternatives considered and rejected

1. **Apollo‑style executor contract (`ApolloRobinhoodExecutor`) adapted for rhpools.**
   Gives an atomic fee with `feeBps ≤ 100`, code‑hash pinning and a single `Executed` event.
   Rejected: out of the assigned direction (no new contracts), and it would make rhpools the
   deployer/owner of a contract holding user funds mid‑transaction, a posture the project has
   documented against (MiniRouter2 retirement). UR already supplies the atomic fee.

2. **Direct venue routers (V2 router `0x89e5…`, V3 SwapRouter `0xcaf6…`) for non‑V4, UR only for V4.**
   Simpler per‑venue calldata (relay‑listener's `execution_plan` does this for single‑venue
   routes). Rejected: no atomic fee without a contract (the fee would need a separate transfer
   tx or a fee‑on‑output the server cannot enforce), plain ERC‑20 approvals instead of Permit2
   (violates the exact‑amount‑with‑expiry invariant), cross‑venue routes impossible in one tx,
   and the ticket would expose venue to the user. Shallow module.

3. **Client‑side encoding with a vendored `universal-router-sdk`/viem.**
   Rejected: the Uniswap SDK encodes the stock structs, which revert or mis‑decode on this UR
   (T3 shows the five‑field struct decoding "by accident" with `minHopPriceX36` read from the
   hookData offset word); a vendored build would need patching and cannot be golden‑tested
   against the fork in CI (`node --check` is the only JS check). Also moves min‑out authority
   into the page.

4. **Quote through the deployed quoters (V4Quoter `0x8dc1…`, V3 QuoterV2) and local V2 math; simulate only for gas/executability.**
   Cheaper per quote and what relay‑listener does. Rejected: two arithmetic paths for one number
   (quoted vs. executed), no rhpools‑fee or hook itemisation without extra reads, and a quoter
   cannot express the fee‑on‑input flow. `eth_simulateV1` returns logs on both Nitro and anvil,
   so the executed bytes are the quote. Kept V4Quoter as a diagnostic cross‑check in tests only
   (T3 proved equality to the wei).

5. **`debug_traceCall`/`callTracer withLog` instead of `eth_simulateV1`.**
   Works on both nodes, but output shape is node‑specific and Nitro adds `beforeEVMTransfers`
   noise; `eth_simulateV1` is the standard method, supports multi‑call staging in one block and
   `traceTransfers` for native ETH movements. Rejected on portability.

6. **Fee taken inside the V4 action set via `TAKE_PORTION`.**
   Rejected: only V4 hops have it; the UR‑level `PAY_PORTION` is uniform across V2/V3/V4 and
   native/ERC‑20 (verified in five shapes).

7. **Use `minHopPriceX36` as the slippage mechanism.**
   Verified semantics (net `amountOut/amountIn × 1e36` per hop) would allow per‑hop floors.
   Rejected: a per‑hop floor is not what the user set; a single min‑out is easier to read in the
   ticket and to audit in calldata. Always 0/`[]`.

8. **Persist quotes in the gate SQLite.**
   Rejected: quotes are 60 s ephemera; a restart losing them costs one re‑quote. Keeps the
   gate DB to identity/policy only, and the market writer untouched.

9. **Route LP through UR's `V4_POSITION_MANAGER_CALL` / `V3_POSITION_MANAGER_CALL`.**
   Rejected: adds a hop with no benefit (no fee on LP actions), and UR whitelists only some PosM
   selectors; direct `PosM.multicall([permitBatch, modifyLiquidities])` and NFPM calls are the
   canonical paths and were executed on the fork (H2–H6).

10. **One‑time unlimited ERC‑20 approval to Permit2 (Uniswap default).**
    Kept as an owner question; the design defaults to exact‑amount because the plan's invariant
    says "exact‑amount Permit2 with expiry" and Robinhood Chain gas makes the extra prompt cheap.

## Red‑flag screen

- Shallow module: no — three verbs hide routing, ABI, staging, accounting, guards.
- Information leakage: the Robinhood ABI lives in one module; wire JSON is built by `to_json()` only; `pools` schema is read in one function of `RouteBook`.
- Temporal decomposition: quote/prepare/receipt are split by *knowledge* (what is bound, what is signed, what happened), and all share `Plan`/`Amounts`; the log interpreter serves both quote and receipt.
- Pass‑through: `lp_server` handlers parse and gate, then call one method; no forwarding layers.

## Things I checked that changed the design

- anvil's default account 0 has an EIP‑7702 delegation on chain 4663 (`0xef0100…`); Permit2 then
  takes the ERC‑1271 path and rejects EOA signatures. The fork harness must use a fresh key.
- USDG is 6 decimals (apollo DECISIONS says 18 — wrong).
- Pons pools accept third‑party liquidity; "hook‑locked" in the plan describes launch liquidity.
- The five‑field V4 struct does **not** always revert; "stock SDK reverts" in third‑party docs is
  data‑dependent. Only the six‑field encoding with golden tests is safe.
