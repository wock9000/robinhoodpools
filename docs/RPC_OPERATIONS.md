# LP RPC operations

RPC configuration belongs to the indexer operator, not to an arbitrary browser request. Public APIs never accept an upstream RPC URL and cannot alter the shared canonical index.

## Credential files

Use `LP_RPC_HEAD_URL_FILES`, `LP_RPC_STATE_URL_FILES`, `LP_RPC_HISTORY_STATE_URL_FILES`, `LP_RPC_LOG_URL_FILES`, `LP_RPC_RECEIPT_URL_FILES`, or `LP_RPC_TRACE_URL_FILES`. Each variable contains comma-separated local filenames. Each file contains HTTP(S) endpoints, one per line, is owned by the service user, and has mode `0600` (or stricter). Files larger than 8 KiB, non-regular files, invalid URLs, and group/world-readable files fail startup without printing their contents.

`LP_RPC_HEAD_WSS_URL_FILES` uses the same ownership, permission, and size checks for files containing WebSocket URLs. Only `ws://` and `wss://` URLs are accepted. Explicit `LP_RPC_HEAD_WSS_URLS` entries precede file-loaded endpoints, followed by `RHP_RPC_WSS` and existing provider fallbacks. `LP_RPC_DISABLE_LOCAL_FALLBACK=1` still excludes local WebSocket endpoints.

Store files outside the checkout. Do not paste a keyed URL into a unit's command line, public example, browser setting, issue, or status report. The file path can safely appear in a systemd drop-in:

```ini
[Service]
Environment=LP_RPC_HISTORY_STATE_URL_FILES=%h/.config/robinhoodpools/archive.url
Environment=LP_RPC_TRACE_URL_FILES=%h/.config/robinhoodpools/archive.url
Environment=LP_RPC_DISABLE_TRACE=0
```

The corresponding `*_URLS` variables remain suitable for unkeyed endpoints and are tried before file-loaded endpoints. `RHP_RPC_URL_FILES` supplies generic fallbacks. Explicit settings are read at service startup; restart after changing them. Alchemy uses an explicitly provided `ALCHEMY_KEY` only; no private environment files are auto-discovered. Preserve `LP_RPC_DISABLE_ALCHEMY=1` if that provider is not intended.

Capability routing checks chain ID 4663 and keeps failure/cooldown state separate for logs, head, current state, archive state, receipts, and traces. A pruned node's missing archive state must not invalidate its valid block headers or logs.

Explicit numbered `eth_getBlockByNumber` reads prefer a configured local head
source. Latest/head discovery keeps the configured remote-first order. A local
null result fails over only the unresolved numbered reads. To qualify only local
headers while keeping general local state fallback disabled, put the local URL
in a private file listed under `LP_RPC_HEAD_URL_FILES` and retain
`LP_RPC_DISABLE_LOCAL_FALLBACK=1`.

Null transaction-hash and receipt lookups continue through the configured
receipt sources. A lagging local node must not make an already observed
transaction permanently unresolved. Successful batch siblings are retained;
an all-null lookup remains null, and a valid null response is not counted as
a provider outage. Transport failures remain errors.

Header batches use the existing 100-item bound and endpoint-wide quota.
Post-log missing headers and the duplicate end-boundary verification share one
batch. Log/header hash checks and the before/after end-hash check still precede
the durable cursor commit. Production scans illustrate the endpoint cost: with
Quicknode first for logs and head, a 96-block scan spent 4.011 s fetching
boundary headers and a 112-block scan spent 17.548 s on per-event headers while
catch-up shared the endpoint; with Goldsky first, a 66-block scan spent 0.100 s
on boundary headers and 0.053 s on event headers. Single-scan samples under
live contention, not percentiles.

### Archive source for the history lane

`LP_RPC_ARCHIVE_URLS` and `LP_RPC_ARCHIVE_URL_FILES` name log and header sources
for the history (archive backfill) lane only. The list is opt-in: no public
provider joins it, `RHP_RPC_URLS` does not feed it, and without it the history
lane keeps the provider log sources above. Prefer the local Nitro node where
it retains block bodies, but do not equate state/archive capability or readable
headers with unlimited historical log retention. Configure a separately
qualified historical log provider as a fallback:

```
[Service]
Environment=LP_RPC_ARCHIVE_URLS=http://127.0.0.1:8547
Environment=LP_RPC_ARCHIVE_URL_FILES=%h/.config/robinhoodpools/goldsky.url
```

Qualification must cover the actual backfill boundary and target, not only
recent logs. Production local reads stopped at `block body not found` just
before block 52,491,468. Goldsky returned five PoolManager logs at 52,491,467;
its header hash matched the durable cursor's parent. It also returned three
logs at the history target, block 29,120,565. This provider remains opt-in;
receipt/state fallbacks do not implicitly enter the archive-only list.

With an archive source the lane fetches each interval as concurrent pages sized
from the observed log density (eight workers against a local host; a page that
reaches the 10k-log safety cap splits in half and retries). Live ingest still
uses the provider sources, and canonical safety holds: interval boundary
headers and the post-log end re-read come from the provider header source,
the archive's own headers for both boundaries must match them, and every log's
block hash must match its event header. A local node whose head is behind the
requested range is skipped for that page rather than trusted.

Archive fetch pages retain their 10k-log safety limit, but history commits stop
at a whole-block boundary near 2,000 logs. History walks backward, so it commits
the newest suffix and leaves the older prefix behind its durable cursor for the
next scan. A single dense block remains indivisible. `history_scan.source` reports
`archive` or `provider`; `recent_catchup_lag_seconds` joins the existing lag
fields. The lane yields to live catch-up when debt exceeds four live batches
or thirty seconds, and rechecks after obtaining writer admission.

For routed HTTP `eth_call` and `eth_estimateGas`, an EVM execution revert is a
contract outcome, not a provider outage. The caller receives the RPC error and
the provider remains available for other calls. Transport failures, malformed
responses, and missing archive state still trigger capability-specific failover.

Routed RPC exceptions retain the redacted JSON-RPC error payload, including
ABI revert data. The pinned-state decoder uses that evidence to recognize
`Invalid token ID` only at a verified same-receipt NFT mint/burn boundary.
Keeping only the provider message loses this proof and retries valid receipts
indefinitely. Unproven absence and unrelated errors still fail enrichment.

Pinned-state batches retain successful siblings when one contract call reverts;
they do not repeat the batch serially. A verified same-receipt mint proves the
parent-block position absent, but the current-block position read remains
required. V3 receipts do not wait for trace capability. Successful receipts
containing V4 PoolManager liquidity modifications still require `callTracer`.

## Quicknode sponsorship

Quicknode supports Robinhood mainnet, chain ID `4663`, over HTTPS and WSS. Keep separate owner-only files for the two endpoint URLs. The token is an endpoint credential, not a platform API key.

```ini
[Service]
Environment=LP_RPC_STATE_URL_FILES=%h/.config/robinhoodpools/quicknode-http.urls,%h/.config/robinhoodpools/goldsky.url
Environment=LP_RPC_HISTORY_STATE_URL_FILES=%h/.config/robinhoodpools/quicknode-http.urls,%h/.config/robinhoodpools/goldsky.url
Environment=LP_RPC_TRACE_URL_FILES=%h/.config/robinhoodpools/quicknode-http.urls,%h/.config/robinhoodpools/goldsky.url
Environment=LP_RPC_HEAD_WSS_URLS=
Environment=LP_RPC_HEAD_WSS_URL_FILES=%h/.config/robinhoodpools/quicknode-wss.urls
Environment=LP_RPC_DISABLE_TRACE=0
```

This example retains an existing Goldsky fallback. Omit that filename if it is not configured. Keep working local header, log, and receipt sources ahead of remote fallbacks. Add the HTTPS file to the corresponding `LP_RPC_HEAD_URL_FILES`, `LP_RPC_LOG_URL_FILES`, and `LP_RPC_RECEIPT_URL_FILES` lists when needed.

`deploy/robinhoodpools-quicknode.conf` also replaces stale explicit head, log, and receipt primaries. Install it as a later service drop-in only after confirming the existing node is unsuitable. A deployment probe found the local node behind the durable cursor, so a successful `eth_chainId` alone was not enough to retain that node as primary.

Before switching, verify chain identity, a pinned historical contract read, receipts, filtered logs, a JSON-RPC batch, and `debug_traceTransaction` with `callTracer`. Verify WSS `newHeads` and log subscriptions separately. A successful head read does not prove archive or trace access.

The sponsored endpoint passed archive USDG `decimals()` at block `30,000,000`, pool state ten million blocks behind the observed head, transaction and call tracing, block receipts, filtered logs, gas estimation, fee history, and `eth_simulateV1`. Simulation does not sign or broadcast transactions. These observations do not establish unlimited retention, a rate allowance, or an SLA.

The router paces unrecognized HTTPS hosts, including Quicknode, at 10 JSON-RPC items per second on average per endpoint with four concurrent requests. Batch envelopes are bounded by one second of the source's admission budget: 10 items for Quicknode, 80 for Goldsky, and up to 100 for unpaced local sources. Successful chunks and valid siblings survive failover. Production Quicknode responses reported a 50-request/second limit; counting HTTP envelopes instead of their RPC items exceeded it. Do not raise admission rates without the account's allowance and a measured workload. WSS traffic is separate and is not included in the HTTP traffic counters.

The [Robinhood credit table](https://www.quicknode.com/api-credits/robinhood) lists 20 credits for ordinary reads and 40 for transaction/call tracing. At a sustained 10 ordinary reads per second, HTTP alone would consume 518.4 million credits in 30 days. This is a workload calculation, not a billing cap or the sponsorship allowance. Streams charge for processed blocks even when filters discard the output.

Back up the affected source files and service drop-ins before deployment. Preserve local-only safety changes, the database path, storage guards, and transaction-preparation settings. Never replace the whole installed directory with an upstream checkout without checking for drift. If the process is recovering a large SQLite WAL, let recovery finish before restarting it for an RPC change.

After deployment, check both loopback and public `/api/lp/status`, all required workers, observed and indexed heads, provider failures, and source traffic. Compare cursor and chain gains over the same interval. Roll back only the changed source files and drop-in if the integration fails. Never delete the database or WAL to recover a provider change.

Quicknode's Robinhood documentation lists no endpoint add-ons. Streams and Webhooks require separate product configuration and platform credentials. The Robinhood endpoint cannot serve Apollo's Solana or Base reads. MEV redistribution is not enabled by this integration.

References:

- <https://www.quicknode.com/docs/robinhood/llms.txt>
- <https://www.quicknode.com/docs/robinhood/endpoint-security>
- <https://www.quicknode.com/docs/robinhood/add-ons>
- <https://www.quicknode.com/docs/streams/rest-api>
- <https://www.quicknode.com/brand>

## Goldsky measurements and limits

A small anonymous-output probe from the production host verified the donated provider privately: chain 4663; exact matching block/header and log digests against the local node; old block headers; USDG `decimals()` at blocks 30,000,000 and 56,400,000; receipts; `debug_traceTransaction` with `callTracer`; and a four-item JSON-RPC batch. The local pruned node could not answer those archive-state calls. Individual successful Goldsky requests in this probe took roughly 76–352 ms; these are samples, not percentile/SLA claims.

Goldsky supports HTTPS JSON-RPC, not WSS subscriptions. Keep the independent WSS
head/activity source. Qualify local capabilities separately rather than assuming
a pruned node can serve archive state. An earlier production probe returned 12
historical/current receipts with matching canonical hashes and three complete
block-receipt results in 32 ms; that evidence does not qualify archive state or
traces.

The current production route keeps Quicknode endpoints paused. Local Nitro
serves available headers, logs, state and receipts; the existing Goldsky file
supplies HTTP fallbacks, including `LP_RPC_TRACE_URL_FILES`. The local node's
historical receipt access does not imply historical execution-state retention:
a sampled transaction receipt succeeded locally while `callTracer` failed with
`required historical state unavailable (reexec=0)`. Goldsky traced that same
transaction successfully. Do not re-enable a billed endpoint merely to restore
trace coverage without checking its allowance.

The donated allowance is 6,000 requests/minute. This process paces Goldsky at at most 80 JSON-RPC items/second on average per endpoint, counting batch elements conservatively, with four concurrent HTTP requests. Bursts are bounded by 80-item envelopes. This leaves nominal headroom under 100/s but does not account for other applications sharing the key. Provider billing/rate accounting remains authoritative.

`/api/lp/status` includes per-source `traffic`: total HTTP attempts, total JSON-RPC items, and rolling approximately 60-second rates. Failed attempts and chain verification count. Traffic is endpoint-wide and repeated under capabilities using that endpoint: **do not sum repeated capability rows**. WSS messages and explorer fallback GETs are not included in these HTTP JSON-RPC counters.

Shared RPC clients no longer serialize complete network requests. Chain
verification is single-flight, and rate waiting does not occupy an HTTP
response slot. Shutdown waits for admitted requests before closing pooled
sessions. Batch failover retains successful siblings and retries only failed
items. `batch_results` exposes failures by input position for trace waves;
ordinary `batch` still raises on non-revert failures.

Goldsky primary references:

- <https://docs.goldsky.com/edge-rpc/introduction.md>
- <https://docs.goldsky.com/edge-rpc/platform/security.md>

## Indexer bottlenecks

Separate durable cursor progress from the live observed feed. A moving live tape is not evidence of complete historical financial accounting. Compare cursor gain and chain gain over the same interval. Live ingestion and historical backfill share one serialized writer; inspect `history_scheduling` and the lane measurements rather than assuming that backfill is paused.

Production sampling found wallet aggregation repeatedly restarting whenever
ingestion advanced its revision. Wallet totals, gas and coverage now come from
one completed WAL read snapshot. Stream consumers reuse the completed snapshot
while a newer revision or an expired window refreshes asynchronously; one
consumer cannot take that snapshot away from another. Blocking wallet reads
still refresh changed revisions and expired windows. Canonical-branch changes
invalidate both cached and in-flight publications.
Cold blocking wallet reads materialize in their existing request thread instead
of waiting behind unrelated background frames. Concurrent reads of the same
view still share one future. Its producer assigns and caches the publication
once, so a waiting request can deliver that exact completed snapshot even if
a stream consumer has already read it.
The browser compares parent hashes only for consecutive block heights.
Skipped head notifications are not fork evidence and must not clear wallet
snapshots. Same-height replacements and confirmed parent mismatches still
withdraw orphaned activity and invalidate wallets.

Selected wallet-page accounting coverage seeks ready pending-identity hints
through `lp_accounting_pending_identities_identity(kind,identity,position_key)`.
It does not expand each wallet through its complete historical event ledger.
Pool, protocol and time-window scope still qualify the same pending position;
incomplete identity hints remain fail-closed. On a populated deployment, allow
for this index build before serving the new query. A production build took
43 seconds with the application stopped and SQLite temporary files on disk,
not tmpfs; that is one measured build, not a startup deadline.

Pool identity recovery tries the pinned getter, canonical receipt evidence and
transaction calldata, then bounded successful PoolManager call traces.
An Initialize receipt already contains the hash-checked full PoolKey and creation
block. It does not need ERC-20 transfers, a PositionManager NFT, or trace state.
A recovered full PoolKey must
hash to the requested pool ID, and its block must still be canonical after the
RPC work. Missing trace capability remains a retryable error, not a guessed
identity. Durable recovery clears the matching current-feed failure while
leaving other unresolved pools visible.
Trace traversal stops once every requested PoolKey in the transaction is
verified. The frame limit bounds the search, not the total transaction size:
unrelated later calls must not discard an already complete result. Searches
that exhaust the budget before finding all requested keys still fail visibly,
and recovered identities still pass the post-trace canonical recheck.

Activity logs leave the replay queue only after successful handling or durable
cursor coverage. If an uncovered backlog exceeds the bounded replay queue, the
subscription reconnects and retains a recovery target; acknowledgement alone
does not clear the error. The durable scanner must cover the dropped interval
before the backlog reports recovery.
Retiring duplicates already covered by the durable cursor records discard
metrics without declaring a new failure. An ordinary pending tail does not
extend a recovery target; only an uncovered queue beyond the bound does.

The live scanner also publishes committed events into the current activity feed
for retained, hash-matched observed blocks. Publication follows the transaction
and checks the canonical epoch under the reorg lock. It does not depend on a
timely WSS log notification. Block hash, transaction hash and log index identify
one event across both sources; late canonical enrichment preserves stronger
receipt facts. Historical enrichment updates existing observed rows rather than
introducing historical activity as a new live event.

Current valuation resolves verified factory metadata as well as token metadata.
Missing decimals remain unknown, not zero; incomplete metadata cannot break the
stream through a missing dictionary field. A feed reset discards the previous
sequence cursor before emitting new IDs. Retaining a higher cursor from the old
process otherwise forces repeated head-only snapshots until the new sequence
catches up, even though the durable index and HTTP endpoints keep advancing.

The browser refreshes durable index status on a one-second heartbeat and visible
aggregate views on an independent three-second timer. A pending overview does
not delay pool or dislocation refreshes. Requests for the same view do not
overlap, cached rows remain visible during refresh, and hidden pages stop their
requests. Aggregate health uses indexed coverage time when available, then frame
time; receiving an old response now does not make its data fresh.

Verify chain-to-screen age on newly displayed rows, not just HTTP duration or a
moving head counter. Exercise a still-open browser across an application restart
and a WAL reader-drain cycle; confirm activity delivery, not just reconnection.

The running service commits raw events, cursors, balance jobs, and coalesced
accounting jobs atomically. A separate accounting worker replays each affected
position from an immutable WAL snapshot without holding the ingestion writer.
It prepares up to 128 positions outside the writer, then shares publication
transactions across their changed rows, releasing the writer after roughly
200 ms of publication work. A newer same-branch generation retains the job
after publishing the coherent snapshot; a changed canonical epoch rejects it.
Existing financial rows remain readable while the queue drains, but cannot be
represented as current accounting.

Header, event, and search writes use parameterized multi-row inserts bounded by
SQLite's variable limit. This avoids handing the Python interpreter to competing
valuation workers between every inserted row. The outer durable transaction
preserves the raw event/cursor/job boundary.

Adaptive scan sizing targets two seconds of live or provider-history writer
work; the archive source targets four seconds. Growth uses measured store
throughput and log density. Live and historical commits stop at a whole-block
boundary near 2,000 logs; an indivisible block may exceed the target. Their
cursors never advance across a fetched but uncommitted portion of the interval.
Writer admission is FIFO within live, normal, and background priorities. Live
work wins the next available transaction, with one background turn after eight
foreground admissions while background work is queued. Enrichment publishes one
completed fetch job per writer turn. History yields above four live batches or
thirty seconds of debt, including a recheck after waiting for the writer.
Price-anchor repair seeks each changed mark's own timestamp window through the
earlier of its next mark or 300-second expiry. It uses the existing timestamp
index rather than scanning from a historical block to the current tip under
the writer. Separated changed marks do not bridge unaffected historical gaps.
Pool-price repairs likewise stop at each changed sample's next price sample.
Replays read canonical samples instead of reusing a current-state price that
the backfill may have invalidated.
A new block can arrive during an in-flight transaction; the reported gap
remains the actual head-minus-cursor difference, without rounding it to zero.
These are feedback targets, not hard transaction deadlines; indivisible block
work can exceed them. Applying a 50 ms target to the live lane shrank it to
one-block commits and left fixed commit costs unamortized.

Wallet freshness reads walk the canonical event-order index after checking
whether the requested scope is empty. A timestamp-range plan sorted the entire
recent ledger before returning its newest event and exceeded 30 seconds on the
production snapshot; the ordered lookup returned the same boundary in under
0.25 seconds including process startup.

Closed-position queries count and select the requested page in one WAL snapshot
with one frozen time cutoff. Only selected episode payloads are materialized.
Fee sorting uses qualified fee values; incomplete-history amounts remain
unknown rather than outranking verified fees. Text filtering retains Unicode
substring matching and literal `%` and `_` characters.

The CLI caps Python's thread-switch interval at 1 ms before starting workers,
preserving an already-shorter interval. This reduces interpreter handoff delays
when SQLite releases the GIL during ingestion while valuation threads run.
In a CPython 3.13 benchmark, median contended ingestion fell from 435 ms to
68 ms, with a 2.1% reduction in valuation throughput. Importing the server
module does not change process scheduling.

Search counts intersect identity-only prefix matches using the term index.
Ranking retrieves each matched token's minimum weight through the existing
entity/weight index. Exact identifiers, normalized labels, token intersection,
weights, and typed-result ordering remain unchanged.

A provider head below the durable cursor pauses live ingestion and reports
degraded source state. Height regression alone cannot delete canonical history.
Reorganization recovery requires conflicting canonical hash or parent evidence.
Rollback repairs the affected search identities from surviving events and pools;
it does not clear the global catalog or restart its historical build.

The terminal's HEAD / INDEX readout, labeled INDEX STATUS on mobile, opens index details. Its timings come from
`live_scan` and `history_scan` in `/api/lp/status`: `fetch_seconds`,
`store_lock_wait_seconds`, `store_seconds`, and the lane's post-processing or
publication duration. Each is one completed batch, not a sustained rate or ETA.

Pool valuation retains at most 128 compact inventories and 100,000 position
triples in total. Changing prices or ticks revalues those positions without
reloading their inventory; metadata and coverage changes still refresh the
result. Oversized pools stream from SQLite without retaining an inventory.
Each pool's inventory generation commits with its accounting changes. Readers
compare generations, marks, metadata, and inventory within one WAL snapshot,
so they can reuse committed caches while a writer is active. Aborted writes
cannot publish a new generation.
Caller-owned read snapshots remain open until the caller ends them.

Schema version 4 replaces the identity-replay queue's single chronological
index with partial indexes for immediate work and timed retries. Startup builds
these indexes without discarding queue rows or changing cursor state. Keep
disk headroom for the migration. A bounded chronological lookup handles an
already-ready queue; otherwise selection excludes future retries before
merging the oldest eligible candidates.

Schema version 5 adds these per-pool generations and an episode activity-time
index for finite-window owner aggregates. Existing events, accounting state,
cursors, and coverage survive the migration. New stores create the same schema.

Schema version 6 adds a partial covering index for open-position inventory.
Inventory reads and LP-count summaries explicitly select this index: production
query plans otherwise preferred the older pool/status/time index and fetched
large position-state rows. The older index remains available for other queries.
Migration preserves accounting state, per-pool
generations, cursors, and coverage; allow space and startup time to build
the new index.

Schema version 7 adds a partial chronological index for financial enrichment,
excluding deferred pool-identity records before the batch limit. Enrichment,
balance, and repricing batches reserve historical work while preferring recent
ready records; a delayed retry no longer stops unrelated repricing. Identity
replay likewise alternates recent and historical work.

Schema version 8 adds a generation to receipt jobs. Completion checks the
canonical block hash and generation inside the same transaction as enrichment;
an older in-flight receipt cannot erase newer identity or retry work.
The accounting queue and its bootstrap checkpoint survive restart without
discarding published positions, episodes, coverage, or cursors.

Schema version 9 records pending accounting scope and actor hints atomically
with source changes, including both sides of NFT transfers. Wallet requests
read those hints and published episodes instead of expanding every queued
position's raw history. Legacy jobs start unqualified; bounded off-writer
pages recover their hints on an independent worker, without waiting behind
full position replays or discarding existing financial rows. Recovery checks
the canonical epoch and per-key cursor before publication.

Schema version 10 adds sparse LP-tape and owner/custody canonical-order indexes.
It does not rebuild accounting or change canonical rows. Tape selects bounded
event IDs before materializing event payloads; owner selection preserves
`COALESCE(owner,custody)` rather than treating custody as a second owner.
Historical owner activity uses the same block/transaction/log ordering.
Allow temporary-sort disk space when installing these indexes on a large ledger.

Schema version 11 adds an `append_only` proof to pending accounting jobs.
Existing jobs default to full replay; accounting state schema remains 3, with
no bootstrap or ledger rebuild. Before rolling back to a pre-11 writer, reset
pending `append_only` values to 0 so that writer cannot leave stale proofs.

Overview and bucket-sorted pool pages share one per-pool interval aggregation,
keyed by event revision, canonical epoch, and the exact bucket boundaries.
An advancing empty block still changes the window boundary. Pool metadata
changes invalidate pool candidates and responses without rescanning buckets.

Owner gas totals resolve exactly attributed transactions through their episode,
even when other transactions require shared-cost qualification. Shared costs
still use the effects relation and are counted once. A production read-only
comparison preserved every value across all-history, 24-hour, protocol and pool
scopes: all-history took 0.43 seconds versus 0.78 for 24,169 owners; a 1,207-owner
pool scope took 0.12 versus 0.41 seconds. The 24-hour query remained about 0.54 seconds.

Legacy V4 identity seeding checks each canonical candidate transaction's queue
entry by transaction hash. It no longer searches every serialized error
payload for a pool ID, and it preserves existing candidate retry schedules.

Receipt enrichment, repricing, and accounting continue during live catch-up.
Receipt fetches run in eight-transaction waves, with up to 64 transactions in
flight. V4 trace work has its own 32-transaction capacity, four-worker pool,
and eight-transaction batches with independent per-transaction outcomes.
Each lane rotates historical, requested, recent, and recent admissions across
refills. Even a one- or two-slot refill must serve requested and current evidence;
sorting a mixed candidate page oldest-first before truncating a trace lane
starves both. In-flight hashes are excluded before candidate limits.
The coordinator refills completed waves before waiting for the database writer.
Receipt fields
provide payer and effective gas price when available; only incomplete receipts
need transaction-body requests. Headers and pinned-state calls are deduplicated
within each wave. The shared provider item-rate and HTTP-concurrency limits
still apply.

Metadata, identity replay, and bounded source repairs run separately from receipt
scheduling. Follow `pending_accounting` as well as enrichment and repricing queues;
a small block gap does not prove those queues are complete.

Metadata RPC fetches remain bounded and commit one batch.
V3 balances have an independent worker rather than waiting behind repricing or
WAL checkpoints. Each wave selects at most 64 exact `(pool,block)` obligations,
reserving a quarter for old history. Multicall3 groups caller-independent reads
by their block pin; balance calls use `blockHash` and `requireCanonical`.
Failed subcalls remain unknown, retain their revert data, and do not discard
successful siblings. Missing or unsupported aggregate responses fall back to
the original pinned calls.
Bulk publication verifies each canonical hash and pending obligation under
the writer, updates latest metadata once per pool, and advances one revision.
No historical snapshots are coalesced or discarded; no fixed per-lane rate
ceiling prevents balances from borrowing unused provider capacity.

A pre-change 122-second production sample created 10.33 balance snapshots/s
and completed only 5.30/s. The new worker completed 128 real-provider snapshots
in 0.775 seconds against an isolated temporary store, with direct canonical
controls matching. This is a smoke measurement, not loaded production capacity.

Identity replay admits up to 64 queue records per pass. Already-known or
rejected identities need no current-state RPC and do not wait for unrelated
live pool discoveries. Each pass admits at most 64 distinct fresh identities,
without taking identities reserved by the current lane. Independent factory
probes share a state wave; membership checks follow in a dependent wave.
V4 PoolKey reads and transaction/receipt/header fallbacks are likewise batched.
Canonical anchor checks precede publication; per-row generation, marker and
stored-hash checks remain inside bounded shared writer transactions. Network
work stays outside the writer. Transient failures retain their obligations;
canonical events and queue completion commit together.
Price sample, reserve, mark, and state inserts use the same multi-row helper
as event ingestion.

Accounting prepares bounded groups in two spawned, read-only processes, with at
most four positions submitted ahead. These workers do not inherit the service's
writer connection, locks or Python interpreter contention from web queries.
Only the existing accounting coordinator publishes: each group gets one
valuation, shared-transaction attribution, episode-cost refresh, and cache
invalidation pass. Preparation and publication target 0.2 seconds each; one
indivisible position can exceed the target. Epoch checks reject reorged work,
and generation-guarded completion preserves same-epoch input that arrived during
preparation. Repricing updates the source revision before projections without
rewriting every event index; search terms refresh only when identity changes.
Effect cleanup is scoped to its prepared position. An upsert can take an effect
from another position only when the current canonical event mapping names that
target; stale remap snapshots cannot remove or reclaim a newer position's gas.
New, previously unprojected events can use the existing incremental append path.
Persisted state must be compatible and the events must start in a later block.
An existing-event correction, remap, uncertain trigger or mixed queue generation
requires full replay; a newer maximum priority does not prove append safety.
Incremental publication preserves untouched historical episodes, effects and
ownership intervals.

Episode IDs remain tied to their opening event when older history arrives;
backfill no longer renumbers every later episode and rewrites its effects.
Deferred rollback preserves opening identities separately from financial rows,
including across a restart before replay.
Replay compares source fields separately from derived valuation and costs,
while receipt changes still refresh dependent gas and net P/L.
Financial-only replay retains derived episode values until revaluation. Gas is
refreshed when transaction-cost evidence or episode/transaction membership changes,
not for every unrelated episode correction. Cost-incomplete episodes stop at the
first missing or inexact transaction; complete episodes still count each transaction
once.
Receipt-only pending work can publish gas and net results without replaying event
history when existing projected events are unchanged, including byte-equivalent
explicit receipt rows. Their normalized source is compared again after preceding
price projections; repricing, explicit corrections, remaps, mixed generations and
uncertain work invalidate that proof and retain full replay. The transient proof
is not serialized into canonical event evidence.

Core-position identities include protocol and pool. An EVM core hash alone is
not globally unique: different pools can share owner, ticks, and salt. Bounded
source repair moves legacy rows to pool-scoped keys without changing event IDs,
receipt evidence, or discovery cursors. Accounting retires a legacy key after
its source mappings have moved rather than replaying the conflated history.
During a partial move, legacy financial rows remain explicitly unqualified;
their overlapping liquidity and fees cannot inflate qualified wallet or pool totals.

Wallet requests reserve bounded receipt and accounting priority without writing
to SQLite on the request thread. Historical work retains its reserved quarter
within each receipt lane. Requested evidence follows the newest renewed wallet
interest; bounded lookup pages stop once enough eligible work is found.
Pool-identity replay likewise reserves a requested turn, so a wallet's deferred
identity prerequisite can reach receipt enrichment without waiting for the
entire chronological backlog.
The current-claims worker admits the full selected owner page, keeps at most 512
wallet interests for 180 seconds, and renews interest when cached API/SSE results
are served. Each pass walks four active positions, four historical position keys,
and bounded transaction pages; it does not scan a wallet's entire history.
Those pages also compare persisted receipt evidence with published gas costs.
Stale attribution is repaired in 32-transaction, epoch-checked writer batches,
so an inactive wallet can recover gas and net P/L without another LP action.

Current claims use canonical end-of-block V3/NFPM or V4 StateView fee growth,
owner and liquidity proof, and integer Q128 fee arithmetic. They borrow at most
eight state RPC items/second from the existing shared allowance. Pool spot prices
qualify only with positive pinned active liquidity. Publication checks the
canonical hash/epoch, absence of pending accounting, and the exact position-state
snapshot; source corrections cannot reuse a claim against different accounting
state. Reorgs clear dependent values before pool-price rollback, rather than
publishing an orphan price. Missing state, trace, gas attribution, acquisition
basis, or price evidence still leaves dependent totals unknown.

The claims table and owner lookup indexes install without rebuilding the ledger.
`current_claims` in `/api/lp/status` reports admitted wallets, refreshed positions,
and the last successful refresh or error.

Existing V3 NFT positions whose initial add precedes a verified mint in the
same transaction are repaired by the separate source-repair lane. A default pass
examines at most 8,192 indexed add events newest-first, selects only affected
positions within reviewed V3-manager ranges, and reprojects at most 32 positions.
The event cursor stops at the last selected repair when the write limit is
reached, so remaining candidates survive the next pass and a restart. This also
repairs final collect-before-burn allocation. The
`v3_birth_history_repair_v2` checkpoint replaces the sparse position-key scan;
the accounting schema version is unchanged, so startup does not replay the
entire ledger. A read-only production probe admitted 16 repairs from 8,192
events in 0.092 s.

V4 source repair correlates already-traced manager positions with canonical NFT
transfers from the same receipt, including receipts without an auxiliary
PositionManager modification event. It preserves the owner at each action's
log order and retains the stored trace, state, cash-flow, and fee evidence.
The bounded `v4_owner_correlation_repair_v1` checkpoint resumes after restart.
Fees before a transfer stay with the prior owner; receiving the NFT does not
prove its acquisition basis.

Wallet-only requests skip the unused custody aggregate. On a warm, read-only
production snapshot of 10,540 wallets, this reduced owner-query time from
1.34 s to 0.78 s; the reported single-wallet query fell from 1.65 s to 0.63 s.
Existing financial fields had identical digests. Retain the short-circuit
missing-gas query: a grouped replacement was slower for the full wallet table.
Complete episode-gas coverage now permits owner costs to qualify their scope
through the attributed episode instead of probing effects per transaction.
Owners with incomplete or shared episode attribution retain the missing-cost
short circuit. A read-only comparison across 48 real wallets returned identical
values and NULLs: all-time gas lookup fell from 21.5 ms to 2.6 ms; the finite
window changed from 67.7 ms to 62.8 ms. These samples are not full-table timings.

For 32 production pools containing 52,393 active positions, selecting the
covering index reduced inventory reads from 0.524 s to 0.093 s and LP-count
summaries from 0.110 s to 0.007 s, with identical output digests.

### SQLite storage

WAL maintenance owns a dedicated connection and worker, independent of projection
and ingestion writer locks. It runs a passive checkpoint every second after the
previous pass finishes. Bulk workers wait for the initial storage assessment;
live ingestion and head observation remain available.

Bulk history, enrichment, projection, metadata, repair, balances and accounting
pause at 512 MiB of uncheckpointed WAL pages and resume at 128 MiB. Allocated WAL
length alone does not pause work because SQLite can reuse checkpointed frames.
At 1 GiB of active WAL pages, or when uncheckpointed backlog reaches the bulk
pause threshold, maintenance closes admission to new analytics snapshots.
In-flight readers get the normal 15-second snapshot budget to finish
before drain cancellation applies; ordinary resets no longer immediately abort
healthy queries. Only snapshots admitted before that drain generation can be
interrupted by it, so readers admitted after fail-open are not canceled by an
expired drain. Default managed snapshots have an independent 15-second lease;
cold owner projections retain their longer ceiling until storage pressure
requires a drain. Expired or interrupted reads roll back and discard their
result; partial rows are not published or cached. Accounting preparation workers
enforce their read budget on their own connections. Writer and caller-owned
transactions are not interrupted. Shutdown still cancels managed reads promptly.
Progress callbacks cannot preempt kernel I/O or Python work between SQLite
operations, so the lease is not a hard wall-clock I/O cancellation guarantee.
Short ordinary reads, status, tape and live ingestion remain available.
Bulk workers also pause during the drain. Cached complete frames remain
available while fresh analytics wait. A drain lasts at most 60 seconds,
followed by a 60-second admission cooldown if it expires.

Once owned snapshots drain, routine maintenance uses `RESTART`, not `TRUNCATE`.
Reader admission closes before competing for the writer. Once owned snapshots
finish, the reset queues a background writer turn, bounded by the existing
drain deadline; it does not rely on finding a gap between continuous writes.
No store or checkpoint lock is held while that turn waits. Expiry reopens
reader admission with the existing cooldown. Unmanaged resets and resets called
inside a writer remain nonblocking.
New application writes wait behind an admitted reset instead of failing
`BEGIN` with `database is locked`. External-reader lock waiting is bounded
to 100 ms. `PASSIVE` checkpointing remains independent of writer admission.

Managed writers use `journal_size_limit=-1`, so the next live insert cannot
silently truncate the allocated WAL after a successful restart. The allocation
is reused; allocated length alone does not trigger maintenance. Explicit
truncation is reserved for planned maintenance, not normal serving traffic.
Successful size-triggered resets have a one-minute cooldown; active backlog
pressure bypasses that cooldown, but an expired reader drain retains its
admission cooldown. The journal is never deleted directly.

The checkpoint connection uses `synchronous=FULL`; the existing writer setting
is `synchronous=NORMAL`. A power failure may lose recent uncheckpointed commits,
which the canonical index must re-derive. The changes above do not alter these
durability settings.

`LP_DISK_RESERVE_GIB` pauses the same bulk workers when free space falls below the
configured reserve. They resume above the reserve plus the larger of 1 GiB or
25%. Set this reserve above any host-level emergency stop threshold. Measurement
or checkpoint errors also pause bulk work until maintenance recovers.

`/api/lp/status` exposes `wal_checkpoint`, `storage_pause_reasons`,
`storage_pressure`, `storage_observed_at`, `storage_resume_bytes` and `bulk_work`.
Checkpoint frame progress and backlog reduction are separate measurements:
ingestion may increase backlog even while checkpointing makes progress.
`wal_checkpoint.log_bytes` measures active WAL pages separately from allocation.
`active_reader_snapshots` counts admitted in-process analytics snapshots, and
`reader_drain_pending` reports the admission gate. Process-worker or external
readers can still make SQLite report busy after the in-process count reaches zero.

Owner scope queries select matching hints and episodes before expanding pending
identities, rather than probing every queued position for each pool query.
Background frame jobs release their reader connections on success and failure.
Failed jobs clear completed traceback locals before a Future can retain an
abandoned cursor and pin the WAL. Caller-owned accounting snapshots remain
consistent until their caller ends them.

Owner financial scope reads use the global indexed canonical boundary rather
than searching unrelated event history for a presentation-filter match.
Historical activity resolves its exact block, transaction and log boundary
through `events_block_idx` instead of expanding every related event key.
Pool summaries use one managed snapshot over the existing pool/bucket/tape read
models. Shutdown signals managed-reader interruption before joining frame jobs.

An oversized WAL can make recovery slow before HTTP is available. Preserve the database, `-wal`, and `-shm` together; never delete the journal to force startup. For planned exclusive checkpoint maintenance, stop the watchdog and all database owners first, let SQLite complete `PRAGMA wal_checkpoint(TRUNCATE)`, verify success, then start exactly one application owner and resume monitoring.

The September 15 recovery checkpoint retained the journal and completed in
239.217 s, returning `[0, 0, 0]` with zero allocated WAL bytes. This is a recovery
receipt, not a database backup or a normal checkpoint latency target.

The effective production settings at the September 19 verification were
`CPUWeight=1000`, `MemoryMax=28G`, and no CPU quota or `MemoryHigh` limit.
Check `systemctl --user show robinhoodpools.service` rather than assuming an
older drop-in still controls these values. Resource limits are not a host-wide
reliability guarantee; compare cursor gain against chain gain under the actual
load. A single fast scan does not establish sustainable catch-up.

Terminal warming retains its next-key position across short writer-idle windows.
It no longer restarts at the first overview key whenever ingestion interrupts
it. Failed keys advance the rotation too and retry after the other default
views have had an opportunity; warming still obeys storage and latency guards.

Published terminal frames use a separate 64-entry cache from the 16-entry cache
of snapshot-versioned pool filters, sorts, bucket aggregates and tape rows.
Obsolete query generations cannot evict otherwise usable terminal responses.
Canonical epoch/revision keys and same-key load coalescing remain in place;
complete frames keep their original `as_of` while a request-triggered refresh
runs.

An admitted snapshot does not wait on a shared query whose producer has not yet
acquired its snapshot: that producer may be blocked by the WAL admission gate.
It reads its own snapshot instead without replacing the other producer's
publication, breaking the snapshot/Future/drain wait cycle.

Pool-ID scans use SQLite JSON aggregation to cross into Python once rather than
once per catalog row. This avoids hundreds of thousands of GIL handoffs during
each metadata-generation refresh. The filtered IDs, ordering and page contents
are unchanged; snapshot deadlines and cancellation remain enabled.

Browser pool requests have a 15-second deadline, including response-body reads.
An initial failed request displays `POOL DATA UNAVAILABLE · RETRYING`, not
indefinite syncing. Completed cached views remain available during refresh.
A real-browser stalled-request probe observed the error state and recovered
100 pool rows on the next request.

Check filesystem capacity and copy-on-write behavior when durable commit time
dominates. A nearly full Btrfs volume can make SQLite's write workload expensive
even on NVMe. For a dedicated non-CoW database directory, set `chattr +C` while
the directory is empty, before copying any database or sidecar files. This
disables Btrfs data checksums and compression for those files. SQLite WAL
checksums and the writer/checkpoint synchronization settings above remain.

The `+C` attribute does not coalesce existing fragmented extents. In a roughly
483 GiB production database, bounded FIEMAP samples near the beginning and
middle had 4 KiB median extents. PASSIVE checkpoints took 11–31 seconds;
kernel samples found extent-tree reads inside database `fsync` and WAL
`pwrite64` waiting on writeback folios. A bounded 64 MiB defragmentation pilot
coalesced the first range into one 64 MiB extent without changing database data.
For diagnosed fragmentation, `btrfs filesystem defragment -f -s OFFSET
-l 1G -t 32M DATABASE` limits each maintenance range. Use background I/O priority
and retain disk headroom. Defragmentation can break shared reflinks and increase
space usage; see the [Btrfs filesystem documentation](https://btrfs.readthedocs.io/en/latest/btrfs-filesystem.html).
Do not delete a journal or weaken synchronization to hide filesystem latency.

Background I/O priority alone does not protect live latency during a sustained
defragmentation pass: the online 1 GiB ranges still interfered with live commits.
Use an approved maintenance window, or smaller ranges gated on index recovery.
For an offline window, stop `robinhoodpools-healthcheck.timer` and its active
one-shot service before stopping the application; otherwise the watchdog
restarts it. Keep free-space checks active, preserve all SQLite sidecars, and
resume the application and any paused supervision timers afterward.

The approved September 20 offline pass completed from the 99 GiB offset to EOF
in 3,707.818 seconds. The 544,497,786,880-byte file retained its size, all 32
sampled 1 MiB SHA-256 digests, and the durable cursor/epoch digest. Bounded FIEMAP
samples found single large extents at the beginning and midpoint and three
extents in the final 64 MiB. The application and both supervision timers were
restored. These checks establish sampled byte preservation, not a full-file
integrity check or database backup.

If a durable commit fails because storage is full or unavailable, the store
rolls the failed transaction back before the worker retries. Runtime-status
persistence is a separate health write. Its failure appears in the live
`errors.status_persistence` field but cannot terminate ingestion, history,
metadata, projection, balance, repair, or accounting loops. The status
`workers` object lists every required index worker and separates running and
missing threads.

After restoring capacity, do not delete SQLite sidecars or attach a
write-capable inspection process. If `workers.missing` is empty, leave the
single owner running and verify that `storage_free_bytes` changes, the
`status_persistence` error clears, and the affected cursor or backlog advances.
An old process that already lost a required thread cannot recreate it. Install
the corrected code, stop that owner, preserve the database, `-wal`, and `-shm`
together, and start exactly one owner.

For relocation, stop the watchdog and owner, verify the copied files by checksum,
compare committed cursor/accounting/coverage state, then update `RHP_DATABASE`
and start one owner. Retain the original files until destination readiness and
index progress are verified. Once the destination accepts new commits, pointing
the service back at the old copy would lose those commits and is not a rollback.

## Availability monitoring and recovery

The independent Cloudflare Worker at <https://status.rhpools.lol/> probes the public root and status API every two minutes. It reports the observed and indexed heads separately from HTTP availability. Its secondary-domain check can fail independently of the application; a registrar hold is not repaired by restarting the indexer.

The user-service units in `deploy/` provide a separate local watchdog. Install `robinhoodpools.service`, `robinhoodpools-healthcheck.service`, and `robinhoodpools-healthcheck.timer` in `~/.config/systemd/user/`, then run:

```sh
systemctl --user daemon-reload
systemctl --user enable --now robinhoodpools.service
systemctl --user enable --now robinhoodpools-healthcheck.timer
```

When migrating an existing installation, preserve its database path with `RHP_DATABASE` in a local service drop-in and stop the previous database owner before starting this service. Keep credential-bearing RPC configuration in separate owner-only files.

The watchdog probes both loopback and the public hostname. A status response with a missing required worker is unhealthy even when the HTTP process still serves requests. Three consecutive local failures are required before an application restart; attempts are limited to three per hour with a ten-minute cooldown. A public-only failure does not restart a healthy indexer. The tunnel is restarted only when its unit is inactive or failed. This intentionally avoids restart loops for DNS, registrar, WAF, or upstream-provider failures. Recovery state is stored outside the checkout under `~/.local/state/rhpools/`.
