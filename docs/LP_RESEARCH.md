# LP wallet research

The LP research surface is a read-only view of the canonical Robinhood Chain LP index. It describes observed allocation, position configuration, fee mode, range width, and lifecycle timing. It does not reconstruct private strategies, infer why a wallet acted, rank wallets by incomplete profit, or submit transactions.

## Web page

Open `/research?owner=0x…&window=30d`. The address must contain `0x` followed by 40 hexadecimal characters.

The page separates:

- positions whose beneficial owner equals the requested address;
- positions that merely use the requested address as custody;
- provisional current activity from durable indexed position history;
- fully valued, partially valued, and unvalued current principal;
- returned-position groupings from claims about the wallet's full history.

The page remains useful while the index is catching up, but it labels the block gap, partial history, source position boundary, and unknown valuation components. An address with no indexed history produces an empty state rather than a zero-allocation claim.

## API

`GET /api/v1/research/owner`

Query parameters:

| Name | Required | Values | Meaning |
| --- | --- | --- | --- |
| `owner` | yes | 20-byte hexadecimal address | Beneficial-owner or custody identity to inspect |
| `window` | no | `1h`, `24h`, `7d`, `30d`, `all` | Episode/activity window; default `30d` |
| `protocol` | no | `v2`, `v3`, `v4` | Restrict returned owner data to a protocol |
| `pool` | no | canonical pool id | Restrict returned owner data to one pool |

Example:

```sh
curl --get 'https://<dashboard-host>/api/v1/research/owner' \
  --data-urlencode "owner=$OWNER" \
  --data-urlencode 'window=30d'
```

Invalid addresses, windows, protocols, and oversized pool ids are rejected with a client error. The endpoint performs no transaction submission and accepts no RPC URL or execution parameters.

## Response contract

The stable top-level fields are:

```text
owner
window
coverage
allocation
configurations
activity
limitations
```

### `coverage`

- `found`: whether returned canonical positions or qualified activity exist in the requested window.
- `state`, `indexed_head`, `history_from`, `history_from_block`, `history_to`, `history_target`: durable index coverage copied from the canonical owner model.
- `window_complete`: true only when indexed history reaches the start of a finite requested window. `all` is conservatively false because the endpoint cannot prove all-chain history.
- `identity.basis`: canonical identity classification such as `verified_owner`, `custody_only`, or `owner_and_custody`.
- `identity.beneficial_owner_observed`: the canonical model has positions beneficially owned by the requested address.
- `identity.custody_observed`: the requested address also appears as custody.
- `identity.current_activity_match`: provisional exact identity match from the current canonical activity suffix, kept separate from durable owner/custody evidence.
- `returned_positions`: position rows included in research groupings, capped at `200`.
- `source_positions_received`: rows supplied by the canonical owner response before the research cap.
- `positions_omitted_by_research_limit`: rows omitted beyond the research cap.
- `source_position_limit`: the canonical owner's base position page boundary, currently `200`.
- `possible_position_truncation`: true when the owner response reaches that boundary. In this state, every allocation and configuration group is explicitly limited to the first 200 returned positions, not a full-wallet aggregate.
- `valuation`: pricing basis plus counts of fully valued, partially valued, and unvalued returned current beneficial-owner positions.
- `current_activity`: coverage of the in-memory canonical activity suffix. Its activity is provisional and is not folded into durable financial totals.

`history_from` and `history_to` are Unix timestamps. Block numbers and timestamps remain ordinary JSON numbers because they are within interoperable integer bounds. Raw token amounts, liquidity, token ids, and raw fee configurations are JSON strings.

### `allocation`

Allocation includes only returned **current beneficial-owner** positions. Custody-only matches are counted in `custody_positions_observed` but never receive a custody-level value subtotal.

- `known_principal_usdg`: sum of known active and pending principal components. It is a known-only subtotal, not a complete wallet value when any component is missing.
- `known_pending_claim_usdg`: known-only pending principal awaiting collection.
- `valued_positions`, `partially_valued_positions`, `unvalued_positions`: component coverage for returned current beneficial-owner positions.
- `pools`: per-pool position counts, known-only USDG principal subtotal, known-only share, per-token raw principal subtotals, and pool inspector link.
- `positions`: position-local liquidity, raw token quantities, known USDG components, valuation mark, and canonical coverage.

`share_of_known_principal_pct` divides a pool's known subtotal by all known returned subtotals. It does not assign a zero value to unknown positions. Raw liquidity is never summed across pools. Per-token raw amounts are summed only within one pool/token group and include completeness counters.

USDG values use the index's established basis: **USDG quote (1 USDG = 1 quote dollar); not a fiat oracle**. Unknown or unpriced components remain `null`.

### `configurations`

Configuration covers all returned positions matching either beneficial-owner or custody identity:

- `positions`: identity match, current/historical state, canonical status, ticks, width in ticks, current in-range observation when a pool mark exists, fee configuration, lifecycle summary, coverage, and pool inspector link.
- `protocol_mix`: counts of returned positions and current beneficial-owner positions by protocol.
- `fee_mode_mix`: returned position and pool counts for `static`, `dynamic`, and `unknown` fee modes.
- `range_width_ticks`: minimum, median, and maximum observed concentrated-liquidity range width. V2 full-range positions are excluded from this summary.

For a static-fee position, `fee.configured_ppm` contains the configured parts-per-million rate. For a V4 dynamic-fee position, `fee.raw_configuration` preserves the dynamic flag, `configured_ppm` is `null`, and `current_ppm` is included only when the current pool state supplies it. A hook address is configuration metadata, not evidence of hook behavior or intent.

### `activity`

- `latest`: the exact matching observation from the bounded in-memory canonical activity suffix when available. Its `qualification` is `provisional_canonical`.
- If no current observation is present, the service falls back to the latest returned position timestamp with `durable_position_summary` qualification. It does not scan the transactions table on request.
- `episodes_observed`: lifecycle episodes attached to returned owner positions.
- `recent_lifecycle`: up to 100 most recent observed episode opens and closes, with identity match, pool, time, and known duration.
- `recent_lifecycle_limit`: currently `100`.

Lifecycle timing is evidence of recorded position events. It is not evidence of automation, a signal, or a decision rule.

### `limitations`

The response repeats the limitations that apply to that request, including partial history, custody aggregation, known-only pricing, the owner-model position boundary, and the absence of execution or intent inference. Consumers should present these alongside any allocation comparison.

## Accounting and identity boundaries

The service reuses canonical position and episode projections. It does not run an unbounded transaction scan, rebuild P/L in the request path, or mix provisional current activity into durable financials.

A custody address can aggregate positions for many beneficial owners. Consequently:

- custody positions remain visible as custody observations;
- custody observations are excluded from beneficial-owner allocation and value totals;
- no manager balance is treated as an individual V4 pool's TVL;
- no custody aggregate is described as a wallet strategy.

The research response intentionally omits owner P/L rankings. Existing canonical episode accounting can contain qualified P/L for other product views, but incomplete histories, unknown trace-derived fee splits, shared or missing gas, transfers, and unpriced tokens make cross-wallet profit comparisons unsafe here.

## Attribution

When redisplaying the response, identify it as canonical indexed Robinhood Chain LP activity, retain the returned coverage and limitations, label USDG as a quote basis rather than a fiat oracle, and link to the corresponding pool inspector for current-state context.
