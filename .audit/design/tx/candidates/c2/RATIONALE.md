# Rationale: immutable executor candidate

## Why a contract at all

The done predicate asks for three things the signer's transaction must carry on its
own: min-out, a deadline, and a fee recipient credited with exactly 75 bps. With no
contract, the fee is a second transfer the user can strip from the calldata, and the
min-out lives in five different router encodings (V2 router, V3 router, UR with
`minHopPriceX36`, PoolManager for hooks, none for Pancake/Giga V3 pools). One immutable
executor gives one `Order` shape, one `Swapped` event, one quoter that is literally the
execution path, and moves the venue quirks behind a surface the auditor reads once.

## Why direct pools, not routers

- The Universal Router on 4663 carries a non-standard ABI without verified source.
  Apollo's own activation audit left its V4 path "unqualified".
- The V2/V3 routers know only the Uniswap factories; Pancake and Giga V3 pools (which
  rhpools indexes and shows) are reachable only by calling the pool.
- Fork proof in `forge-poc/`: direct `PoolManager.unlock/swap/settle/take` executes a
  Pons buy and sell from a plain contract, and the returned delta already includes the
  hook's two floor-divided fees; V3 direct swaps succeed on Uniswap, Pancake and Giga
  pools using the two callback names.

## Alternatives considered and rejected

1. **Router calldata built server-side, fee as a separate step.** Rejected: fee not
   atomic/enforceable; per-venue calldata; UR ABI risk moves to the server and the user.
2. **Executor wrapping the Universal Router (Apollo's shape).** Rejected: inherits an
   ABI we cannot verify, cannot reach non-Uniswap V3 pools, still needs ETH plumbing.
3. **Off-chain quoting (relay-listener `/v1/quote`, or local tick math).** Rejected as
   the primary path: second implementation of the Pons fee model that must track the
   hook; Rust service dependency. Kept as an optional cross-check, not a dependency.
4. **Per-call fee bps with an on-chain cap.** Rejected: a user can zero it by editing
   calldata; enforcement evaporates. Immutable `feeBps` instead; rebates (deferred)
   settle off chain.
5. **Owner-rotatable fee recipient.** Rejected: reintroduces an admin key and a
   privilege the audit must reason about; a redeploy plus host-config change is the
   rotation path and leaves old pins valid.
6. **On-chain pool/token allowlists.** Rejected: the user signs their own calldata and
   can only harm themselves; allowlisting is the server's job and would grow the
   contract's state and audit surface for no third-party protection.
7. **LP through the executor.** Rejected: the canonical managers already enforce mins
   and deadlines, and NFPM/POSM own the NFT; a middleman adds custody risk and audit
   scope with no fee to collect.

## Audit burden, honestly

- Surface: ~300–350 lines; hot spots are `unlockCallback` (must be PoolManager),
  the two V3 callbacks (must be the pool set in transient storage), `receive` (only
  WETH and PoolManager), native-ETH accounting, Permit2 pull with a try/catch permit,
  the residue check, and `quoteExactIn`'s nested revert/catch. Reentrancy is closed by
  a transient lock; the contract holds no funds between calls.
- Known unrecoverables: dust from fee-on-transfer outputs; a fee recipient that
  starts rejecting ETH bricks native-fee swaps until a redeploy.
- Cost: roughly one external auditor week; the pool/hook interactions are standard
  Uniswap patterns (QuoterV2, v4 quoter, v4 router settle/take) an auditor has seen.
- Mitigation before the report lands: server-side `amountIn` cap, "unaudited" badge in
  the ticket, and the reproducible code hash + published standard JSON so anyone can
  diff the deployed bytecode against source.
