# LP market storage and pruning, 2026-09-27

## What is actually consuming space

The live `~/.local/share/rhpools-nocow/lp_market.sqlite` had 161,202,854 pages of 4,096 bytes, or 660.29 GB, when inspected through a read-only SQLite URI. `freelist_count=0`, `auto_vacuum=0`, and `sqlite_stat1` does not exist. The database is in WAL mode. A later `stat` showed the actively growing file at 660.93 GB and its WAL at 2.47 GB. These measurements are moving targets, not a frozen snapshot. The observed whole-database increase is approximately 14 GB/day. Nothing in the existing database is unallocated free space.

The estimates below use only indexed endpoint probes and at most 32 leaf pages of SQLite's `dbstat` per named object. One sample from the oldest B-tree branch can differ substantially from the rest of the object. `max(rowid)` is a **high-water upper bound**, not a row count, on tables with deleted rows or sparse event IDs. In particular, the prices, accounting, and search-index estimates cannot be added as though every event has a row in every table. A full `dbstat` aggregation or `COUNT(*)` over these enormous tables was deliberately avoided. Representative read-only commands:

```sh
python - <<'PY'
import sqlite3, pathlib
p = pathlib.Path.home()/'.local/share/rhpools-nocow/lp_market.sqlite'
c = sqlite3.connect(f'{p.as_uri()}?mode=ro', uri=True, timeout=1)
c.execute('PRAGMA query_only=ON')
print(c.execute('PRAGMA page_size').fetchone(), c.execute('PRAGMA page_count').fetchone(), c.execute('PRAGMA freelist_count').fetchone())
print(c.execute("SELECT ncell,payload FROM dbstat WHERE name='events' AND pagetype='leaf' LIMIT 32").fetchall())
print(c.execute('SELECT id,timestamp FROM events ORDER BY id DESC LIMIT 1').fetchone())
PY
```

Observed first-leaf densities, translated into approximate table sizes or **upper bounds** using the high-water ID, in decimal GB:

| Table | Indexed high-water / sampled cells per page | Size indication | Keep for |
| --- | ---: | ---: | --- |
| `events` | 188.14m IDs / 4.6 | ~167 GB | Tape, all-time activity, projection and accounting replay |
| `lp_price_samples` | event ID 188.14m / 27 | <=29 GB | Historical price repair and pool state |
| `lp_v2_reserve_samples` | event ID 188.14m / 30 | <=26 GB | Historical reserve repair |
| `lp_accounting_effects` | rowid 188.13m / 9.4 | <=82 GB | Open-ended position PnL |
| `lp_accounting_tx_costs` | rowid 189.21m / 7.4 | <=105 GB | Owner gas and PnL |
| `lp_pool_buckets` | rowid 92.62m / 17.8 | <=22 GB; actual much lower | 60s/3600s window edges and permanent 86400s all-time totals |
| `lp_search_terms` | rowid 403.39m / 46.6 | <=36 GB | Search |
| `blocks` | block 51.81m through 74.27m / 20.4 | ~4.5 GB for the 22.46m block span, not 15 GB from max block | Time-to-block mapping, reorg, coverage |
| `transactions` | rowid 6.65m / 9 | <=3.1 GB | Gas and tape |
| `lp_accounting_pending` | ID 20.97m / 23 | <=3.8 GB | Outstanding accounting work |
| `lp_accounting_episodes`, `lp_accounting_positions` | rowid 9.46m / 4.1, 5.23m / 2.1 | <=9.5 GB, <=10.2 GB | Open and closed positions |
| `pool_balances`, `lp_price_marks`, `lp_owner_rollup_journal`, `lp_ownership_intervals` | rowid 14.7m / 24.5, 38.94m / 19.1, 9.28m / 15.7, 2.62m / 10.4 | <=2.5, <=8.4, <=2.5, <=1.1 GB | Current/historical balances, USD anchors, accounting |
| Other small tables (`pools`, `pool_provenance`, `coverage_intervals`, `metadata`, `pending_*`, catalog/search entities) | Small or high-churn; no reliable row count without a scan | Not estimated separately | Operational state |

Secondary indexes also occupy real pages. The same 32-leaf-page sample yields these approximate **upper bounds**, assuming one index entry per 188.14m events unless the index is partial. Partial indexes can be much smaller. All amounts are decimal GB. `sqlite_autoindex_events_1` ~35.5; `events_pool_time_idx` ~7.0; `events_kind_id_idx` ~6.4; `events_owner_time_idx` <=22.6; `events_custody_time_idx`, `events_owner_order_idx`, `events_custody_order_idx`, `events_position_order_idx` each smaller partial indexes (not individually sized); `events_block_idx` ~4.3; `events_lp_order_idx` <=6.4 (partial); `events_revision_id_idx` ~5.4; `events_tx_log_idx` ~16.9; `lp_samples_order` <=19.8. For `lp_pool_buckets`, 32 leaf pages held 16.5 table rows/page, 45.5 PK entries/page and 43.8 `lp_buckets_pool` entries/page. Its hourly/day covering indexes held 34.2 entries/page each. Other index footprints (accounting, search, transactions and samples) remain unquantified individually; do not treat the above as a partition that totals 660 GB. Exact per-object sizing requires a full `dbstat` pass on an offline clone, not the live writer.

Indexed bounds put `blocks` between timestamps 1,788,275,187 and 1,790,543,541, ~26.25 days and ~855k blocks/day. Events reached ID 188,136,791 during investigation, ~8.3m IDs/day over its ~22.6-day populated-ID range. Minute buckets span 1,788,275,160 through 1,790,543,520, ~26.3 days. Indexed bucket counts observed during the reader audit were 27,822,331 minute, 3,822,317 hour, and 1,096,498 day rows, about 1.06m minute, 146k hour and 42k day rows/day averaged over this span. Because backfill is still active, daily rates are approximate. The 60s and 3600s bucket table and PK/pool/covering indexes together are roughly 15 GB at this age, ~0.55 GB/day of the ~14 GB/day total. No 35-day-old buckets yet exist in the live DB.

## What the application needs

`lp_server.py` dispatches HTTP routes to `LPMarketService` and `MarketStore`. `lp_market_service.py:25,2054-2110` serves `1h`, `24h`, `7d`, `30d`, and `all`. Bucket reads use minute rows at each window's partial-hour edge, hourly rows at each partial-day edge, and daily rows for complete days. `all` starts at Unix epoch 0 and requires permanent daily rows, but it does not need old minute or hourly rows. The 35-day buffer preserves all 30-day edge reads even around an hourly/day boundary and clock skew. `lp_market_service.py:3381-3516` still serves the historical tape from canonical `events`; owner/closed APIs in `lp_market_accounting.py` require open-ended effects, position intervals, costs and transactions. Price sample/mark tables support arbitrary historical reprojections. Do not silently truncate any of those source or ledger tables. `blocks` and coverage endpoints support reorg and time mapping, not just a 30-day dashboard.

A more substantial cold-storage plan would first split canonical `events` plus dependent `transactions`, blocks, price samples, and accounting ledgers into an addressable, immutable archive, then teach tape, owner, search and reprojection readers to consult both stores. Nothing in this change does that. There is no safe blanket 30-day deletion of events or accounting data, however tempting the roughly 660 GB headline may be.

## Retention choices by table family

| Tables | Proposed policy | Why |
| --- | --- | --- |
| `lp_pool_buckets` | Retain 60s and 3600s rows for 35 days; retain 86400s rows indefinitely | Exact rolling-window edges and all-time totals without keeping every old minute |
| `events`, `transactions`, `blocks`, `coverage_intervals` | Keep online until the historical tape and reorg/coverage readers can address a verified cold archive | Canonical source and open-ended tape; dropping one alone breaks readers and replay |
| `lp_price_samples`, `lp_v2_reserve_samples`, `lp_price_marks`, `lp_pool_state` | Keep; consider cold storage only together with historical reprojection and previous-sample lookups | Backfilled pricing can seek any earlier block |
| `lp_accounting_effects`, `lp_accounting_tx_costs`, `lp_accounting_episodes`, `lp_accounting_positions`, `lp_ownership_intervals`, `lp_owner_rollup_journal`, other `lp_accounting_*` | Keep; cold storage needs an owner query and replay cutover first | PnL, closed positions and ownership are open-ended |
| `pool_balances`, `pools`, `pool_provenance`, `token_metadata`, search/catalog tables | Keep until consumer-specific archive/rebuild contracts exist | Current catalog and historical balances/search need those records |
| `pending_*`, `metadata` | Preserve outstanding queue work and progress | Already lifecycle state, not event history to age out blindly |

Sampled upper bounds are not daily growth measurements. The observed per-day rates are ~855k new block IDs and ~8.3m event IDs over their populated ranges, ~1.06m minute, ~146k hourly and ~42k daily bucket rows across the 26-day bucket span. Price/reserve samples, accounting effects/costs and search terms have sparse or churned rowids, so their exact row/day increments and per-index daily bytes cannot safely be inferred from `MAX(rowid)` alone. Their share of the ~14 GB/day total remains an estimate until a bounded, repeated snapshot of each table's indexed counter exists. Quoting a per-table exact growth number for them would invent precision the live database does not support.

## Implemented bounded retention

The service's existing maintenance thread schedules one `MarketStore.prune_bucket_batch` after healthy storage maintenance, only during 02:00-04:59 UTC and while bulk work is allowed, or when low free space is the only pause reason: freed bucket pages land in the freelist, which the storage reserve counts as headroom, so that pause must not stop the job that relieves it. WAL backlog, WAL ceiling, reader drain and assessment pauses still stop it. Each attempt takes the existing **background-priority** single-writer turn without waiting and deletes at most 512 `(resolution,bucket,pool_id)` keys in one atomic transaction. It walks the existing `(resolution,bucket,pool_id)` primary-key index with a persisted JSON cursor in `metadata`; a restart resumes at the committed key. Completing each resolution resets its cursor, so later historical backfills cannot remain hidden behind an old cursor forever. Two empty batches back off for five minutes. It touches only `resolution IN (60,3600)` and `bucket < now-35 days`. No schema migration or extra index is needed. A pruning error is logged and retried without being misclassified as WAL-checkpoint failure or stopping the writer.

Pruning a minute does **not** decrement its already materialized hour/day sums. For late historical event ingestion or reprojection at a pruned timestamp, `PriceProjection._buckets` recomputes each affected hourly and daily total once per projection batch directly from canonical `events`, indexed by `(pool_id,timestamp)`, inside the same projection transaction. The rollback path also locates affected old events whose minute rows were pruned, and rebuilds their hour/day sums using only events through the common ancestor. This avoids adding partial minutes twice or leaving orphaned totals. Tests exercise a 40-day-old backfill after restart, a deep rollback into pruned history, and 30-day/all-time API results.

At today's observed ~26-day oldest bucket, **immediate deletion and reclaimed bytes are zero**. After enough history accrues, steady-state pruning retires about 1.2m minute/hour rows/day, roughly 0.55 GB/day of B-tree pages, and the DB would grow near **13.45 GB/day instead of ~14 GB/day**, subject to backfill and actual page packing. This is a useful derived-data bound, not a cure for the canonical-data growth rate. The theoretical old 26-day minute/hour set is roughly 14 GB, but it is all younger than the conservative cutoff and cannot safely be reclaimed by this job today.

SQLite's `auto_vacuum=0` means deletion creates freelist pages **inside** the main database; it does not reduce its apparent file size or immediately return `df` space. Future inserts and checkpoints can reuse those pages and slow growth. WAL writes generated by pruning must be checkpointed/reset by the existing maintenance lane; long-lived readers can delay WAL truncation. `PRAGMA incremental_vacuum` cannot reclaim file pages without rebuilding the DB with `auto_vacuum=INCREMENTAL`; merely running the pragma now does nothing. Actually shrinking the file requires an offline SQLite rebuild (`VACUUM INTO`/logical export into a new filesystem file), enough headroom for the replacement, an exclusive cutover, and controlled snapshot/WAL handling. This machine had approximately 548 GB free, less than a second complete ~660 GB copy; **do not attempt an online rebuild**. On btrfs, a deleted file's physical extents become free only after every reflink/snapshot reference and open descriptor releases them and delayed references settle. No production service, database, backup, or worktree was modified during this investigation.

## Other retained artifacts

`btrfs filesystem du -s /home/andnasnd/.local/state/rhpools /home/andnasnd/h/wt/rhpools-*` reported 8.82 MiB exclusive for the state directory. The two old 2026-09-20 worktrees consumed 13.75 MiB exclusive (`rhpools-data-lifecycle-20260920`) and 3.65 MiB exclusive (`rhpools-resource-efficiency-20260920`). Today's token-utility, prune, V3 and V4 worktrees were reflink-shared with zero exclusive MiB at measurement; flow-tags and tx-core had 11.95 MiB and 12.38 MiB exclusive. The state directory contains 2026-09-20 source snapshots and small backup manifests, plus stray healthcheck temp files. None was deleted here.

Separately, another operator deleted two 563 GB reflink-linked predeploy backups under `.local/state/rhpools/data-lifecycle-20260920/` after verifying offsite copies. Each had shown 524.40 GiB shared and zero exclusive; btrfs Data Used and `df` did not immediately improve. Independent `sudo -n btrfs filesystem du -s` probes over the broad `/home` tree, selected top-level directories, and even the single live DB file timed out after 18-60 seconds without totals. GNU `du -sxB1 /home/andnasnd/.local/share` reported 766,010,490,880 bytes, including 89,842,393,088 bytes in `.local/share/apollo`, roughly 661 GB of live SQLite main-file allocation and ~2.5 GB WAL. `findmnt -T /home` shows `/dev/nvme0n1p3[/home]` while `/var` is `/dev/nvme0n1p3[/root]`; filesystem-wide `df` also includes that separate root subvolume. Neither a remaining reflink reference nor deferred btrfs extent/discard processing is established as the cause of the missing free-space increase. The other operator's bounded `find / -xdev -size +50G` and `find /home -xdev -size +50G` found no second file over 50 GB besides the live DB. Do not budget 524 GiB as recovered until filesystem-wide usage actually falls.

## Raw sample retention, 2026-11-02

### Where the space is

Page counts per B-tree from a read-only walk of the live database's interior pages through `sqlite_dbpage` on 2026-11-02 (events and blocks exact; other objects estimate leaf counts from 512 evenly spaced parent pages, within 0.1% of the exact count on `blocks`). Table plus its indexes, decimal GB: `events` 575.9 (table 362.7, `sqlite_autoindex_events_1` 59.2, `events_pool_time_idx` 41.7, `lp_events_pool_order` 32.1, `events_tx_log_idx` 30.0), `lp_price_samples` 68.0 (table 41.3, `lp_samples_order` 26.7), `lp_accounting_effects` 40.2, `lp_search_terms` 24.7, `lp_pool_buckets` 23.2, `lp_accounting_positions` 20.3, `lp_accounting_episodes` 20.2, `lp_price_marks` 16.6, `lp_accounting_event_keys` 14.4, `pending_enrichment` 13.8, `blocks` 12.8, `lp_accounting_tx_costs` 9.1, `pool_balances` 8.2, `transactions` 8.0, `lp_v2_reserve_samples` 1.4. `lp_pool_state` (0.4) holds one row per pool; its high rowid comes from `INSERT OR REPLACE`. `lp_owner_rollup_journal` held about 18k rows; #33 already drains it.

### Decision

Only the two raw sample tables are derived data that nothing needs in full:

- A `lp_price_samples` row is a copy of one event with `sqrt_price_x96` (its order, timestamp, sqrt, and the ratio `_sqrt_sample_ratio` computes from the pool's decimals). A `lp_v2_reserve_samples` row is a copy of one V2 event whose `data` carries `reserve0`/`reserve1`, with the ratio `_v2_sample_ratio` computes. Pool decimals are only ever filled in, never changed, and filling them in re-queues every event of the pool.
- Readers ask four questions: the latest sample of a pool (V2 TVL, dislocations, routing), the sample at or before a position (projection of events without their own price, V3 TVL at the balance block, window baselines), the first sample after a position (repair range after a backfilled source), and a pool's first sample (the `all` baseline).

Retention keeps every sample from the first block with an event at or after `now − 35 d`, and for each pool its first sample and its newest sample before that block. Every at-or-before answer at or above the block is therefore unchanged, and so is every latest/first answer. API windows reach back at most 30 days. Lookups below `sample_prune_block` (projection of old events during backfill, reorg replay or re-pricing, and an inactive pool's V3 TVL) read the canonical event through `lp_events_pool_order` instead (`PriceProjection.sample_at_or_before` and `_sample_after`). A re-priced old event whose sample was pruned counts as changed, so it re-queues the events up to the next sample. That repeats repair work the unpruned database skips, which bumps revision counters more often, but every value comes out the same.

Every other large table stays in full: `events`, `blocks`, `transactions` (canonical tape, reorg and replay input), every `lp_accounting_*` table and `lp_ownership_intervals` (open-ended PnL, ownership, costs), `lp_price_marks` (cross-pool USD anchors replayed during reprojection), `lp_search_*` and the `pending_*` queues.

### Mechanism

`MarketStore.prune_sample_batch` visits at most 256 pools and deletes at most 512 rows per transaction, walking `(pool_id,block_number,tx_index,log_index)` with a cursor in `metadata` (`sample_prune_cursor`) and raising `sample_prune_block` before the first delete of a sweep. A dedicated `lp-market-sample-prune` thread runs batches for 0.25 s, each waiting for a background-priority writer turn (the scheduler admits background writers between foreground bursts), then rests three times as long, so it stays under a quarter of wall time; it rests an hour after a full sweep. A first deploy ran the batches non-blocking from the maintenance tick, and under live catch-up they never got the writer: no batch ran in five minutes. The thread uses the same storage gate as bucket pruning (`_retention_prune_allowed`). `--sample-retention-days` / `LP_SAMPLE_RETENTION_DAYS` sets the window; `0` turns pruning off. No schema change.

### Proof

`tests/test_sample_retention.py` builds 41 days of V2, V3, V4 and cross-pool history with LP positions opened and closed across the cutoff, copies the database and prunes one copy. API answers (overview, pools under every sort, tape, owners, closed, positions, owner, dislocations, for every window) and the accounting and projection tables match before and after pruning. They still match after re-pricing only the sample-reading events, an archive backfill below all history, token metadata re-pricing every event, a live day and a reorg. Without the event fallback, the re-pricing step changes `lp_accounting_effects`.

The same comparison ran against a bounded production slice: six USDG pools (2 V2, 2 V3, 2 V4) with 45 days of their events (25,116 events, 13,412 accounting effects), rebuilt through the project's own ingest. 92 API answers and ten ledger tables were identical after pruning. After re-pricing 19,069 events (5,631 event-fallback lookups), only opaque revision counters differed.
