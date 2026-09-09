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

## Goldsky measurements and limits

A small anonymous-output probe from the production host verified the donated provider privately: chain 4663; exact matching block/header and log digests against the local node; old block headers; USDG `decimals()` at blocks 30,000,000 and 56,400,000; receipts; `debug_traceTransaction` with `callTracer`; and a four-item JSON-RPC batch. The local pruned node could not answer those archive-state calls. Individual successful Goldsky requests in this probe took roughly 76–352 ms; these are samples, not percentile/SLA claims.

Goldsky supports HTTPS JSON-RPC, not WSS subscriptions. Keep the existing independent WSS head/activity source. Route cheap local headers/logs locally and use Goldsky for archive state, receipts/traces, current state, and fallback. Enrichment remains bounded; enabling archive access does not instantly complete historical ownership or P/L.

The donated allowance is 6,000 requests/minute. This process paces Goldsky at at most 80 JSON-RPC items/second on average per endpoint, counting batch elements conservatively, with four concurrent HTTP requests. Bursts are bounded by the batch size (100). This leaves nominal headroom under 100/s but does not account for other applications sharing the key. Provider billing/rate accounting remains authoritative.

`/api/lp/status` includes per-source `traffic`: total HTTP attempts, total JSON-RPC items, and rolling approximately 60-second rates. Failed attempts and chain verification count. Traffic is endpoint-wide and repeated under capabilities using that endpoint: **do not sum repeated capability rows**. WSS messages and explorer fallback GETs are not included in these HTTP JSON-RPC counters.

Goldsky primary references:

- <https://docs.goldsky.com/edge-rpc/introduction.md>
- <https://docs.goldsky.com/edge-rpc/platform/security.md>

## Indexer bottlenecks

Separate durable cursor progress from the live observed feed. A moving live tape is not evidence of complete historical financial accounting. Compare cursor gain and chain gain over the same interval. Live ingestion and historical backfill share one serialized writer; inspect `history_scheduling` and the lane measurements rather than assuming that backfill is paused.

Production sampling found wallet aggregation repeatedly restarting whenever ingestion advanced its revision. This could keep LP Wallets at `SYNCING` indefinitely while consuming CPU needed by the indexer. Wallet totals, gas and coverage now come from one completed WAL read snapshot; appends trigger a later refresh instead of recursive recomputation. Canonical-branch changes still invalidate service publications. The regression exercises an indexer append during an actual wallet read.

Late historical events rebuild only the affected position's changed accounting rows rather than deleting and rewriting its entire projection. This preserves synchronous, atomic financial updates while reducing write amplification. Existing durable metadata counters are reused on restart; full-table counts initialize missing counters only.

Header, event, and search writes use parameterized multi-row inserts bounded by
SQLite's variable limit. This avoids handing the Python interpreter to competing
valuation workers between every inserted row. The outer durable transaction and
its atomic accounting/cursor boundary are unchanged.

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

Metadata RPC fetches use the bounded enrichment workers and commit one batch.
V3 balances use a batched pinned-state request and atomic snapshot commit.
Identity replay commits canonical events and queue completion together.
Price sample, reserve, mark, and state inserts use the same multi-row helper
as event ingestion.

Existing V3 NFT positions whose initial add precedes a verified mint in the
same transaction are repaired by the normal projection lane. A default pass
examines at most 8,192 indexed add events newest-first, selects only affected
positions within reviewed V3-manager ranges, and reprojects at most 32 positions.
The event cursor stops at the last selected repair when the write limit is
reached, so remaining candidates survive the next pass and a restart. This also
repairs final collect-before-burn allocation. The
`v3_birth_history_repair_v2` checkpoint replaces the sparse position-key scan;
the accounting schema version is unchanged, so startup does not replay the
entire ledger. A read-only production probe admitted 16 repairs from 8,192
events in 0.092 s.

Wallet-only requests skip the unused custody aggregate. On a warm, read-only
production snapshot of 10,540 wallets, this reduced owner-query time from
1.34 s to 0.78 s; the reported single-wallet query fell from 1.65 s to 0.63 s.
Existing financial fields had identical digests. Retain the short-circuit
missing-gas query: a grouped replacement was slower for the full wallet table.

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
