# Transaction core arena synthesis

Base: candidate 1 (UniversalRouter only, no new contracts, eth_simulateV1
quotes). Cross-judge (reviewer) scored C1 27, C2 17, C3 15 and recommended C1.
C3 is disqualified: the live relay-listener quote service refuses Pons routes,
and the rhpools token is a Pons launch. C2's executor, the part that carries the
fee and min-out, is unwritten and was planned to ship before an audit.

## Grafts and fixes (mandatory)

| Source | Change | Why |
|---|---|---|
| judge | Fee reconciliation: on OUTPUT-fee plans `net_out = pool_out − hook − creator − rhpools_fee`; apply Pons floors to the hooked hop's own Swap log, not the last hop | As written, every sell and every bridged Pons sell is refused as `unmodeled_fee` |
| C3 | `tx_allowlist.py`: `{address: code_hash}` for UR, Permit2, PosM, PoolManager, the three NFPMs and the Pons hook; checked at startup and at prepare against latest; mismatch disables trade and lp | Plan invariant; a re-etched or proxied target must not go unnoticed |
| judge | Fork cases the probes skipped: expired deadline (UR `0x5bf6f916`), LP min+1 reverts, signed `multicall([permitBatch, modifyLiquidities])`, V3 and V4 increase, Pancake and Giga NFPMs exercised | Done predicate 6 is not proven by the probes |
| C1/C3 | Browser e2e and fork tests use a fresh EOA funded with `anvil_setBalance`; anvil accounts 0-2 carry EIP-7702 code on 4663 and Permit2 routes their signatures through ERC-1271 | Otherwise every permit step reverts |

## Owner-facing defaults (decided, reversible)

- Pons pools: remove and collect allowed; adds refused. Pons pools have fee 0 in
  the PoolKey and the hook keeps swap fees, so a third-party LP earns nothing.
- Fee basis: 75 bps of the quote currency, input on buys, output on sells.
- Approvals: one-time ERC-20 → Permit2 approval (Uniswap standard), then an
  exact-amount PermitSingle to the UR with expiry = quote deadline per trade.
- Quote TTL 60 s; the open ticket re-quotes silently every 10 s.

## Rejected

- C2 custom executor: new unaudited contract in the money path for Pancake,
  Giga and Slipstream swap coverage. Those venues stay LP-able via NFPMs.
- C3 listener quote engine: cannot quote Pons; couples trading to apollo's
  collector restarts.
- C1 `minHopPriceX36` per-hop floors: kept at 0; one user slippage bound.
