# Public LP data API

The Robinhood Pools public API is anonymous, read-only, and available at
[`https://rhpools.lol`](https://rhpools.lol). Its canonical machine-readable
contract is the [OpenAPI 3.1 document](https://rhpools.lol/api/v1/openapi.json).

The API exposes four `GET` routes:

| Route | Purpose |
| --- | --- |
| `/api/v1/pools` | Every verified known pool containing a token, with current block-pinned state |
| `/api/v1/assets` | Unit-safe configuration groups and defensible V2 reserve subtotals |
| `/api/v1/research/owner` | Bounded, coverage-qualified owner allocation, configuration, and lifecycle research |
| `/api/v1/fomo/flow` | One bounded, attributed page from a selected anonymous public flow publisher |

No route requires authentication. JSON responses include
`Access-Control-Allow-Origin: *`; browser clients do not send credentials. The
routes do not accept RPC URLs, submit transactions, recommend trades, or infer
private strategies.

## Run an installed local service

Install the package, then start the installed `rhpools` command:

```sh
python -m pip install .
rhpools --host 127.0.0.1 --port 8196
```

The default local origin is `http://127.0.0.1:8196`. The command uses the
official public RPC and user-local state defaults unless their documented CLI
options are supplied. The same OpenAPI document is served locally at
[`http://127.0.0.1:8196/api/v1/openapi.json`](http://127.0.0.1:8196/api/v1/openapi.json).

## Token lookups

Both token endpoints require one `token` query parameter: a `0x`-prefixed
20-byte currency address. Input hex is case-insensitive and output is normalized
to lowercase. The native currency uses the zero address.

```sh
curl --fail --get 'https://rhpools.lol/api/v1/pools' \
  --data-urlencode 'token=0x0000000000000000000000000000000000000000'

curl --fail --get 'https://rhpools.lol/api/v1/assets' \
  --data-urlencode 'token=0x0000000000000000000000000000000000000000'
```

### `GET /api/v1/pools`

A pool matches when the queried address is either `currency0.address` or
`currency1.address`; `matched_currency` identifies the side. The route returns
every verified known match. There is no output pagination: supplying `limit` or
`offset` is a `400` error rather than a silent cap.

The response contains:

- `chain_id`, the normalized `token`, `pool_count`, and `pools`;
- `snapshot`, the exact current block number, hash, timestamp, canonical
  confirmation basis, and state-read transports;
- `coverage.catalog`, which qualifies the verified known census and any omitted
  malformed or conflicting catalog records;
- `coverage.history`, which separately describes indexed canonical event
  history; and
- `coverage.state`, which counts available and unavailable current liquidity
  reads.

“Every verified known match” does not mean every pool deployed on-chain.
`coverage.catalog.complete_for_known_catalog` applies only to supported known
factories and the indexed V4 PoolManager catalog.

Verified pool identities are cached separately from current liquidity snapshots
and invalidated when pool or token metadata changes. Recovering an omitted V4
tick spacing reuses candidates from previously verified PoolKeys, but each
candidate must still reproduce the requested pool hash. Large-token responses
remain complete rather than silently capped; state reads use bounded RPC
batches and retain the exact-block confirmation described above.

### Pool identity and dynamic fees

- V2 and V3 have a 20-byte `pool_id` equal to `pool_address`.
- V4 has a 32-byte `pool_id`, `pool_address: null`, and a separate singleton
  `manager_address`. The manager contract is not a pool.
- V4 `pool_key` is the exact identity tuple: `currency0`, `currency1`, raw
  24-bit fee configuration, signed tick spacing, and hooks address. The tuple is
  checked against the 32-byte pool id.
- `fee.configured_raw` preserves the exact configuration integer as a decimal
  string. For dynamic V4, `8388608` (`0x800000`) is a flag, not an 8,388,608 ppm
  fee, so `configured_ppm` is null.
- A dynamic pool's `current_ppm` is read separately at the response snapshot
  block. `current_status` reports whether that read is block-pinned or
  unavailable.
- A missing fixed fee is `mode: "unknown"` and remains null; it is never assumed
  to be zero.

### Liquidity units and unavailable state

Interpret liquidity only according to `liquidity.type`:

- `v2_reserves`: `reserve0_raw` and `reserve1_raw` are exact currency raw-unit
  integer strings from `getReserves()` at the snapshot block. Apply the matching
  currency's `decimals` only for display.
- `concentrated_active_liquidity`: `active_liquidity_raw` is protocol raw active
  $L$ from V3 `liquidity()` or V4 StateView `getLiquidity(poolId)`. It is
  pool-specific, not a token amount, TVL, or value, and must not be summed across
  pools.

Chain-sized and raw integers are decimal JSON strings where the schema says so,
preventing JavaScript precision loss. Small counters, token decimals, and
research observations explicitly typed as JSON numbers remain numbers.

A successful zero is `"0"`. A failed, reverted, or malformed contract read has
`status: "unavailable"`, null value fields, and an `unavailable_reason`.
Unavailable state is never converted to zero. One unavailable pool does not
discard successful reads for other pools.

### `GET /api/v1/assets`

This route reuses the pools endpoint's canonical snapshot and groups matches by:

1. protocol;
2. fee mode and exact configured fee field;
3. tick spacing; and
4. V4 hooks address.

Each `groups[]` row contains the normalized configuration, exact pool ids,
measured/missing state counts, `subtotals`, and limitations.

The only current cross-pool subtotal is `matched_currency_reserve` for V2. It
sums the queried token's reserve side across measured V2 pools in the group.
Every term therefore has the same currency and raw unit. `value_raw` is exact;
`value_decimal` is non-null only when token decimals are consistent and known.
The subtotal's `coverage` is `partial` if any pool in the group lacks a reserve
measurement.

The route never:

- sums V3/V4 raw active $L$ across pools;
- combines counterpart reserves denominated in different currencies;
- converts amounts using an unpinned or fabricated price;
- attributes PoolManager balances to an individual V4 pool; or
- turns missing state into zero.

## Owner research

`GET /api/v1/research/owner` requires `owner`, a `0x`-prefixed 20-byte address.
It accepts:

| Parameter | Required | Values and behavior |
| --- | --- | --- |
| `owner` | yes | Owner or custody address; normalized to lowercase |
| `window` | no | `1h`, `24h`, `7d`, `30d` (default), or `all` |
| `protocol` | no | `v2`, `v3`, or `v4` |
| `pool` | no | V2/V3 address or exact V4 pool id, at most 128 characters |
| `pool_id` | no | Accepted alias of `pool`; `pool` wins when both are supplied |

```sh
curl --fail --get 'https://rhpools.lol/api/v1/research/owner' \
  --data-urlencode 'owner=0x0000000000000000000000000000000000000000' \
  --data-urlencode 'window=30d'
```

The response is intentionally bounded to 200 source positions and 100 recent
lifecycle entries. Check `coverage.possible_position_truncation`,
`coverage.positions_omitted_by_research_limit`, and each returned row's coverage
qualifiers before deriving totals.

`allocation` covers returned current positions matched as beneficial ownership.
Custody matches are counted and described separately; they are not given a
custody-level value subtotal because one custody address may represent multiple
beneficial owners. Raw liquidity remains position-local and is never added
across pools.

`known_principal_usdg`, `known_pending_claim_usdg`, and related fields are
known-only observations. Their explicit basis is “USDG quote (1 USDG = 1 quote
dollar); not a fiat oracle.” Unknown fees and unpriced tokens are excluded and
remain null rather than becoming zero.

Current canonical activity and indexed historical coverage are distinct.
`coverage.window_complete: false` means the index does not prove complete
coverage of the requested period. The `all` window is never marked complete.
Provisional current activity is labelled `provisional_canonical`; durable
position summaries are labelled separately.

## Public Fomo flow

`GET /api/v1/fomo/flow` accepts exactly these query parameters:

| Parameter | Required | Values and behavior |
| --- | --- | --- |
| `source` | no | `apollo` (default) or `rhtrenches` |
| `chain` | no | `solana`, `base`, or `robinhood`; source constraints apply |
| `limit` | no | Integer from 1 through 100; default 50 |
| `cursor` | no | Opaque Apollo base64url cursor, 1–4096 characters |
| `verified` | no | `true` or `false`; default false; Apollo only |

Unknown parameters are `400` errors.

Apollo covers its supported `solana`, `base`, and `robinhood` set. An omitted
`chain` means that supported set, not every chain. Continue a bounded Apollo
page with `next_cursor` when non-null. `verified=true` restricts the page to
publisher-verified Fomo identity.

RH Trenches is Robinhood Chain only. It rejects non-Robinhood `chain` values,
`cursor`, and `verified=true`; an omitted chain is normalized to `robinhood`.
Its response is a current bounded tape snapshot and has no older-cursor
contract.

```sh
curl --fail --get 'https://rhpools.lol/api/v1/fomo/flow' \
  --data-urlencode 'source=apollo' \
  --data-urlencode 'chain=robinhood' \
  --data-urlencode 'limit=50'
```

The response preserves publisher identity kind, source evidence, evidence time,
decimal-string economics, provenance, coverage, and limitations. Publisher
warning rows and RH Trenches non-cash-leg estimated-value rows are omitted and
counted in `coverage.rows_omitted`. These are published observations, not intent
or recommendations. The selected public contracts do not provide an approved
exact same-transaction pool route, so trades are not attributed as LP activity.

Use `occurred_at` and, when available, `observed_at` as evidence times.
`read_at` is server read time and is not evidence time.

## Freshness and completeness

For pool and asset responses, the service obtains the Robinhood Chain head,
executes contract calls using that exact block number, and confirms the block
hash again before publication. An otherwise valid response may be briefly
cached; use `snapshot.block_number`, `snapshot.block_hash`, and
`snapshot.timestamp`, not HTTP arrival time, as freshness evidence.

Coverage dimensions are independent:

- catalog coverage qualifies known-pool discovery;
- history coverage qualifies indexed canonical events and backfill;
- state coverage qualifies the current block-pinned liquidity calls;
- research coverage qualifies identity, position-page bounds, valuation, and
  the requested historical window; and
- flow coverage qualifies a selected publisher's bounded page, chain scope,
  identity scope, omissions, and evidence.

Preserve nulls and these qualifications in derived data.

### Terminal wallet accounting

The terminal's `/api/lp/owners`, `/api/lp/owner`, and `/api/lp/closed`
responses distinguish complete collected-fee totals from partial evidence:

- `fees_usd` is available only when the selected beneficial-owner episodes
  have complete history, fee attribution, and pricing. Unknown totals remain
  `null`, as do gross/net P/L values without their required evidence.
- `observed_collected_fees_usd` sums independently priced collected-fee
  observations even when the complete total is unavailable. It is not a
  complete total, a guaranteed lower bound, or fiat-oracle USD.
- `coverage.observed_collected_fees` declares `unit: "USDG_quote"`,
  `episodes`, `history_complete_episodes`, `total_episodes`, and `complete`.
  The terminal labels nonzero partial observations **OBS**. Custody alone
  does not attribute these earnings to a beneficial owner.
- A finite `window` selects episodes by their last activity, then aggregates
  their lifetime quantities. Coverage states
  `episode_selection: "last_activity_within_window"` and
  `financial_scope: "lifetime_of_selected_episodes"`. Do not treat these
  values as cash flows earned exclusively during the requested window.
- Owner-list `financials.through_order` is the canonical
  `[block_number, transaction_index, log_index]` boundary captured in the same
  WAL snapshot as the financial aggregates. `financials.as_of` describes that
  boundary, not the time of the latest observed activity. Pending projection
  work or later activity keeps `financials.pending` true without advancing the
  captured boundary. A completed snapshot is distinct from complete history,
  trace attribution, pricing, and fee evidence.

Fresh canonical activity does not imply complete transaction enrichment or
historical accounting. Preserve both freshness and completeness qualifications.

## Errors

All errors use a JSON object with an `error` string.

| HTTP | Meaning |
| --- | --- |
| `400` | Missing, malformed, unsupported, or source-incompatible query input |
| `503` | Bounded request capacity, required current/indexed service, canonical confirmation, or selected public publisher is unavailable or unusable |

A canonical block changing before publication is `503`; retry the whole request.
No result from that attempt is published. Individual pool contract-call
failures remain a `200` response with explicit per-pool nulls and reasons.
Capacity errors may include `Retry-After`.

Deployment edge limits may additionally return `429`. Clients should honor
`Retry-After`, use exponential backoff, and avoid aggressive polling.

## Attribution

When publishing analysis from pool or asset responses, cite:

- Robinhood Chain id `4663`;
- the endpoint and queried token;
- `snapshot.block_number` and `snapshot.block_hash`;
- catalog, history, and state coverage for their stated scopes; and
- the exact liquidity type and units used.

For owner research, preserve beneficial-owner versus custody identity,
position-page bounds, history-window completeness, and the USDG quote basis.
For flow data, preserve publisher/source attribution, identity kind, chain
scope, omissions, and evidence timestamps.
