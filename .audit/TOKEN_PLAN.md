# rhpools token utility: plan

Base: `feat/token-utility-20260927` at `1b00978`, a snapshot of the deployed
production tree (231 tests pass). Production had drifted from every git branch;
this snapshot is the only faithful base.

## Decisions (owner, 2026-09-27)

- PONS = trade in a Pons-launched token (pool registered in the Pons V2 hook).
- Tags: FOMO mark on the existing FEED tape (37 of 144 recent LP rows are
  FOMO-routed), plus a new TRADES section. PONS never appears on FEED: Pons pools
  had 0 LP rows in the sample. The hook does not block adds (permissions are
  beforeInitialize/afterSwap only, fork-verified); third parties simply don't LP
  them, so PONS flow only exists as trades.
- rhpools trade fee: 75 bps, collected atomically in the swap, shown in the ticket.
- Rebates deferred until legal review.
- Pons fees, measured over 549 live Pons pools: hook fee 100 bps on every pool;
  creator tax frozen per launch, 0-500 bps (0, 100, 200 most common). A buy of a
  200 bps token through rhpools costs 100 + 200 + 75 = 375 bps before impact.

## Done predicate

1. Anonymous requests to every existing route return the same responses as the
   snapshot (golden compare) and the existing suite passes.
2. Non-holder: gated quote/prepare/key/tag requests are refused server-side.
3. Holder at or above threshold (stand-in ERC-20 on an anvil fork): signs in,
   sees tags, mints a key, streams with it; after selling below threshold, loses
   access once the grace window ends.
4. Only an owner-signed message changes the policy; any other signer is refused
   and every change leaves an audit row.
5. On the fork, a holder buys and sells a Pons token and a V3 token from the
   ticket: min-out holds, the fee recipient receives exactly 75 bps, and expired
   quotes and slippage reverts surface as clear states.
6. On the fork, V3 NFPM and V4 PositionManager mint, increase, decrease and
   collect reconcile balances to the wei.
7. Tags: PONS exact on a labeled sample; FOMO agrees with relay-listener
   observations on at least 99% of a 200-trade sample.

Rigor: high. Real funds, a public launch, and key/session formats that API
consumers will depend on.

## Facts that shape the design

- Server is stdlib `ThreadingHTTPServer`, vanilla JS, strict CSP, cloudflared
  path allowlist. No auth, sessions or API keys exist. Public JSON is `ACAO: *`.
- Documented invariant: rhpools never holds keys, signs, or broadcasts. The only
  transaction path builds unsigned V3 burn/collect calldata. Add-liquidity via
  MiniRouter2 was retired as unsafe.
- Wallet code exists only in `static/workbench.js` (raw EIP-1193).
- Token will be a Pons V2 launch (Uniswap v4 hook `0xe5e7…e044`). "Coin fees" are
  the creator share of the Pons swap fee (measured 190 bps creator, 100 bps hook),
  swept by the Pons operator to the creator address. There is no claim call.
- On chain 4663, verified by bytecode: Permit2 (canonical), V4 UniversalRouter,
  V4 PositionManager (uses canonical Permit2), V3 NFPM, V2/V3 routers, Pons hook.
- 20k-block sample: 573 active Pons pools, 12,463 Pons swaps. Top `tx.to`:
  FOMO Relay router `0xccc8…15be` (2,539), unknown `0x6505…40dc` (1,966),
  ERC-4337 EntryPoint (1,799), UniversalRouter (1,132).
- relay-listener collector is live: 1.93M `fomo_observation` rows in 24h in
  apollo Postgres. Nothing in rhpools reads it. No PONS signal exists anywhere.
- The terminal has no swap tape. FEED is LP actions; `/flow` proxies apollo and
  rhtrenches FOMO flow.
- Pipeline health: `pending_enrichment` 7.87M and `pending_accounting` 1.83M are
  still growing. DB is 652 GB, growing ~13.6 GB/day; the 160 GiB storage guard
  trips in ~28 days.

## Data shapes

```
Feature        = trade | lp | api | flags
GatePolicy     = token, decimals, threshold[Feature], grace_s, version   (owner-signed)
Holding        = wallet, balance_raw, block, observed_at                  (balanceOf, 30 s cache)
Entitlement    = wallet, features ⊆ Feature, holding
Session        = id_hash, wallet, expires                                  (SIWE, HttpOnly cookie)
ApiKey         = key_hash, wallet, label, created, revoked_at              (shown once)
FlowTag        = tx_hash, tag ∈ {PONS, FOMO}, basis, early: bool
```

Gate state lives in its own small SQLite file, never in the 652 GB market DB.
The owner address is pinned in the systemd unit. The web can't change it. Policy
changes are EIP-712 messages signed by that address, with an audit row per change,
plus a host CLI fallback.

## Units, in order

| # | Unit | Proof |
|---|------|-------|
| 0 | Gate core: policy, owner signature, balance oracle, gate DB | pytest vs anvil fork with a stand-in ERC-20; non-owner signature rejected |
| 1 | Wallet sign-in (SIWE) + header status in theme | browser run: connect, sign, holder vs non-holder state, cookie flags |
| 2 | API keys; key-gated REST limits and SSE/WS stream | curl with/without key; stream closes after balance drops past grace |
| 3 | PONS/FOMO tags, live path only, gated fields | tag precision on labeled sample; non-holder gets no tag field |
| 4 | Trading: server-built unsigned swaps, eth_call simulated, user signs | anvil fork end to end with browser wallet; slippage/expiry/revert paths |
| 5 | LP: mint, increase, decrease, collect (V3 NFPM, V4 PositionManager) | fork end to end per protocol; mins enforced; hook-blocked adds refused |
| 6 | Rebates | design only until legal review |

Unit 0 blocks everything. After 1, units 2 and 3 run in parallel (disjoint
files). Units 4 and 5 share one transaction core (quote, simulate, prepare,
receipt); that core lands first, then they split.

## Invariants

- Server never holds keys or signs. Every transaction is built server-side,
  simulated at a pinned block from the user's address, and signed in the wallet.
- Every swap/LP transaction carries min-out/min-amount and a deadline. Approvals
  are exact-amount Permit2 with expiry. Targets come from a pinned allowlist with
  code-hash checks.
- The anonymous site and public API behave exactly as today. Gating adds, never
  removes.
- Gate checks fail closed for gated features and never touch the market writer.
- Tags come from live-window transaction data, not the enrichment backlog.

## Risks

- Revenue share to holders is the construct apollo's own docs refused without
  legal review. A usage-based trading rebate carries less risk than pro-rata
  payouts; neither ships before counsel.
- Gating protects rhpools services only; anyone can call the routers directly.
- Balance flaps during a holder's own sell: grace window, not instant revoke.
- Launch traffic lands on a pipeline whose queues are not converging and a disk
  that fills in about four weeks.
