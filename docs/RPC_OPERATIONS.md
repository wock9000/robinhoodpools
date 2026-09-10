# LP RPC operations

RPC configuration belongs to the indexer operator, not to an arbitrary browser request. Public APIs never accept an upstream RPC URL and cannot alter the shared canonical index.

## Credential files

Use `LP_RPC_HEAD_URL_FILES`, `LP_RPC_STATE_URL_FILES`, `LP_RPC_HISTORY_STATE_URL_FILES`, `LP_RPC_LOG_URL_FILES`, `LP_RPC_RECEIPT_URL_FILES`, or `LP_RPC_TRACE_URL_FILES`. Each variable contains comma-separated local filenames. Each file contains HTTP(S) endpoints, one per line, is owned by the service user, and has mode `0600` (or stricter). Files larger than 8 KiB, non-regular files, invalid URLs, and group/world-readable files fail startup without printing their contents.

Store files outside the checkout. Do not paste a keyed URL into a unit's command line, public example, browser setting, issue, or status report. The file path can safely appear in a systemd drop-in:

```ini
[Service]
Environment=LP_RPC_HISTORY_STATE_URL_FILES=%h/.config/robinhoodpools/archive.url
Environment=LP_RPC_TRACE_URL_FILES=%h/.config/robinhoodpools/archive.url
Environment=LP_RPC_DISABLE_TRACE=0
```

The corresponding `*_URLS` variables remain suitable for unkeyed endpoints and are tried before file-loaded endpoints. `RHP_RPC_URL_FILES` supplies generic fallbacks. Explicit settings are read at service startup; restart after changing them. Alchemy uses an explicitly provided `ALCHEMY_KEY` only; no private environment files are auto-discovered. Preserve `LP_RPC_DISABLE_ALCHEMY=1` if that provider is not intended.

Capability routing checks chain ID 4663 and keeps failure/cooldown state separate for logs, head, current state, archive state, receipts, and traces. A pruned node's missing archive state must not invalidate its valid block headers or logs.

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

## Goldsky measurements and limits

A small anonymous-output probe from the production host verified the donated provider privately: chain 4663; exact matching block/header and log digests against the local node; old block headers; USDG `decimals()` at blocks 30,000,000 and 56,400,000; receipts; `debug_traceTransaction` with `callTracer`; and a four-item JSON-RPC batch. The local pruned node could not answer those archive-state calls. Individual successful Goldsky requests in this probe took roughly 76–352 ms; these are samples, not percentile/SLA claims.

Goldsky supports HTTPS JSON-RPC, not WSS subscriptions. Keep the independent WSS head/activity source. Route cheap local headers/logs locally. An explicit `LP_RPC_RECEIPT_URLS` can also put the local node ahead of the archive fallback: a production probe returned 12 historical/current receipts with matching canonical hashes, plus three complete block-receipt results, in 32 ms. This does not establish archive-state or trace availability; those capabilities remain separately routed to Goldsky.

The donated allowance is 6,000 requests/minute. This process paces Goldsky at at most 80 JSON-RPC items/second on average per endpoint, counting batch elements conservatively, with four concurrent HTTP requests. Bursts are bounded by the batch size (100). This leaves nominal headroom under 100/s but does not account for other applications sharing the key. Provider billing/rate accounting remains authoritative.

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

The browser refreshes durable index status on its one-second heartbeat,
independently of the slower overview refresh. Requests do not overlap and
are aborted while the page is hidden. The moving chain head is no longer
compared against an index cursor held back by a twelve-second overview timer.

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

Adaptive scan sizing uses separate writer targets: two seconds for live
catch-up and 200 ms for background history. Growth is capped by measured store
throughput and log density; history starts with eight blocks before adapting.
History yields when the recent gap exceeds one adaptive live batch, rather than
requiring the durable cursor to equal a continuously advancing head. Exact-tip
admission starved history even while live ingestion stayed only a few blocks
behind. The adaptive batch is the amount the live worker can commit next, not
an unrelated fixed lag allowance.
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

Accounting prepares bounded groups off the writer and publishes each group with
one valuation, shared-transaction attribution, episode-cost refresh, and cache
invalidation pass. Preparation and publication target 0.2 seconds each; one
indivisible position can exceed the target. Epoch checks reject reorged work,
and generation-guarded completion preserves same-epoch input that arrived during
preparation. Repricing updates the source revision before projections without
rewriting every event index; search terms refresh only when identity changes.
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

WAL checkpoints run outside the ingestion writer lock. The writer retains at most 256 MiB of reusable journal allocation after a safe reset; this is not a hard cap on active transactions or snapshots. A reader may still pin older WAL frames until its snapshot finishes. A real SQLite smoke kept an old reader at one row while 530 large rows committed, then safely reclaimed the journal after that reader ended; all 532 final rows survived reopen.

An oversized WAL can make recovery slow before HTTP is available. Preserve the database, `-wal`, and `-shm` together; never delete the journal to force startup. For planned exclusive checkpoint maintenance, stop the watchdog and all database owners first, let SQLite complete `PRAGMA wal_checkpoint(TRUNCATE)`, verify success, then start exactly one application owner and resume monitoring.

Check filesystem capacity and copy-on-write behavior when durable commit time
dominates. A nearly full Btrfs volume can make SQLite's write workload expensive
even on NVMe. For a dedicated non-CoW database directory, set `chattr +C` while
the directory is empty, before copying any database or sidecar files. This
disables Btrfs data checksums and compression for those files; SQLite WAL
checksums and `synchronous=FULL` remain in use.

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

The watchdog probes both loopback and the public hostname. Three consecutive local failures are required before an application restart; attempts are limited to three per hour with a ten-minute cooldown. A public-only failure does not restart a healthy indexer. The tunnel is restarted only when its unit is inactive or failed. This intentionally avoids restart loops for DNS, registrar, WAF, or upstream-provider failures. Recovery state is stored outside the checkout under `~/.local/state/rhpools/`.
