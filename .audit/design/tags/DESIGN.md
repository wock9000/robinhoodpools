# Flow tags (unit 3)

Architect skipped: the shape follows from the existing event model. A tag is a
pure function of (pool registration, transaction envelope, listener
observations). No competing structure changes the caller's view.

## Data shape

```
Tag        = PONS | FOMO
TagBasis   = pons_hook | fomo_router | fomo_user_op | fomo_listener
FlowTag    = tx_hash, tags: frozenset[Tag], basis: frozenset[TagBasis],
             early_ms: int | None     # listener saw the FOMO buy this long before the fill
TxEnvelope = tx_hash, block_number, from_, to, type, user_ops: tuple[UserOp, ...]
```

`classify(pool: PoolIdentity, env: TxEnvelope, observed: ListenerFacts | None) -> FlowTag`
is pure. Everything else feeds it.

## Rules

- PONS: the event's pool is a V4 pool whose hook is the Pons V2 hook
  `0xe5e7…e044` and `launches(poolId)` word 0 is nonzero. Registration is frozen
  per launch; cache forever per pool id.
- FOMO, on chain: `tx.to` is the FOMO Relay router `0xccc8…15be`; or `tx.to` is
  the ERC-4337 EntryPoint and a `UserOperationEvent` sender matches the FOMO
  footprint decoded by relay-listener (`fomo/robinhood.rs` in
  `apollo-listener-integration-20260920/workers/relay-trade-listener`: executor,
  depository, delegation implementation). A generic EntryPoint call or generic
  7702 code alone is not FOMO.
- FOMO, listener: a `fomo_observation` (chain robinhood, status observed, not
  retracted) with the same transaction hash. `early_ms` = fill block time minus
  the earliest listener observation for the same order (Solana payment or Relay
  deposit), when that is positive.
- Agreement check: on-chain FOMO and listener FOMO are compared per trade; the
  disagreement rate is exported in status, not hidden.

## Data flow

- Only live-window events are classified: swaps (TRADES) and LP actions (FEED)
  published after startup, plus a bounded backfill of the most recent N minutes.
  The 7.9M-row enrichment backlog is never consulted.
- Envelopes: one batched `eth_getTransactionByHash` per publication batch
  against the local node; receipts only for EntryPoint transactions.
- Tags persist in their own small SQLite (`tags.sqlite`, 7-day retention). The
  market DB and its writer are never touched.
- Listener facts: optional read-only Postgres source (`RHP_LISTENER_DSN`,
  `SET TRANSACTION READ ONLY`, statement timeout). Down or unset means on-chain
  rules only; status reports the source state.

## Surfaces

- Existing responses are not changed. Holders with `flags` get tags from a
  separate batch endpoint (`/api/v1/tags?tx=…`, ≤ 200 hashes) and a `tags` event
  on the keyed stream; the terminal merges them into FEED rows and TRADES rows
  client-side.
- TRADES: a new terminal section next to FEED, same KeyedTable and row style,
  swaps from the existing event stream. Columns: age, side, pool, amount USDG,
  tag, tx. Anonymous users see TRADES without the tag column content (a dim
  lock glyph in the header cell links to sign-in).
- Mark: text badge `PONS` / `FOMO` using the existing `.badge` class, uppercase,
  one colour each from the theme tokens. `FOMO` with `early_ms` shows `FOMO +1.4s`.

## Verification

- Pure `classify` tests from recorded envelopes (real tx hashes from the probe).
- Precision sample: 200 recent swaps; PONS must equal hook registration exactly;
  FOMO on-chain vs listener agreement ≥ 99% on the overlap, disagreements listed.
- Golden: existing routes byte-identical.
- Browser: TRADES renders live rows; holder sees tags; non-holder sees the lock.
