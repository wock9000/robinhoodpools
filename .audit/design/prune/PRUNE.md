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

The service's existing maintenance thread schedules one `MarketStore.prune_bucket_batch` after healthy storage maintenance, only during 02:00-04:59 UTC and while bulk work is allowed. Each attempt takes the existing **background-priority** single-writer turn without waiting and deletes at most 512 `(resolution,bucket,pool_id)` keys in one atomic transaction. It walks the existing `(resolution,bucket,pool_id)` primary-key index with a persisted JSON cursor in `metadata`; a restart resumes at the committed key. Completing each resolution resets its cursor, so later historical backfills cannot remain hidden behind an old cursor forever. Two empty batches back off for five minutes. It touches only `resolution IN (60,3600)` and `bucket < now-35 days`. No schema migration or extra index is needed. A pruning error is logged and retried without being misclassified as WAL-checkpoint failure or stopping the writer.

Pruning a minute does **not** decrement its already materialized hour/day sums. For late historical event ingestion or reprojection at a pruned timestamp, `PriceProjection._buckets` recomputes affected hourly and daily totals directly from canonical `events`, indexed by `(pool_id,timestamp)`, inside the same projection transaction. The rollback path also locates affected old events whose minute rows were pruned, and rebuilds their hour/day sums using only events through the common ancestor. This avoids adding partial minutes twice or leaving orphaned totals. Tests exercise a 40-day-old backfill after restart, a deep rollback into pruned history, and 30-day/all-time API results.

At today's observed ~26-day oldest bucket, **immediate deletion and reclaimed bytes are zero**. After enough history accrues, steady-state pruning retires about 1.2m minute/hour rows/day, roughly 0.55 GB/day of B-tree pages, and the DB would grow near **13.45 GB/day instead of ~14 GB/day**, subject to backfill and actual page packing. This is a useful derived-data bound, not a cure for the canonical-data growth rate. The theoretical old 26-day minute/hour set is roughly 14 GB, but it is all younger than the conservative cutoff and cannot safely be reclaimed by this job today.

SQLite's `auto_vacuum=0` means deletion creates freelist pages **inside** the main database; it does not reduce its apparent file size or immediately return `df` space. Future inserts and checkpoints can reuse those pages and slow growth. WAL writes generated by pruning must be checkpointed/reset by the existing maintenance lane; long-lived readers can delay WAL truncation. `PRAGMA incremental_vacuum` cannot reclaim file pages without rebuilding the DB with `auto_vacuum=INCREMENTAL`; merely running the pragma now does nothing. Actually shrinking the file requires an offline SQLite rebuild (`VACUUM INTO`/logical export into a new filesystem file), enough headroom for the replacement, an exclusive cutover, and controlled snapshot/WAL handling. This machine had approximately 548 GB free, less than a second complete ~660 GB copy; **do not attempt an online rebuild**. On btrfs, a deleted file's physical extents become free only after every reflink/snapshot reference and open descriptor releases them and delayed references settle. No production service, database, backup, or worktree was modified during this investigation.

## Other retained artifacts

`btrfs filesystem du -s /home/andnasnd/.local/state/rhpools /home/andnasnd/h/wt/rhpools-*` reported 8.82 MiB exclusive for the state directory. The two old 2026-09-20 worktrees consumed 13.75 MiB exclusive (`rhpools-data-lifecycle-20260920`) and 3.65 MiB exclusive (`rhpools-resource-efficiency-20260920`). Today's token-utility, prune, V3 and V4 worktrees were reflink-shared with zero exclusive MiB at measurement; flow-tags and tx-core had 11.95 MiB and 12.38 MiB exclusive. The state directory contains 2026-09-20 source snapshots and small backup manifests, plus stray healthcheck temp files. None was deleted here.

Separately, another operator deleted two 563 GB reflink-linked predeploy backups under `.local/state/rhpools/data-lifecycle-20260920/` after verifying offsite copies. Each had shown 524.40 GiB shared and zero exclusive; btrfs Data Used and `df` did not immediately improve. Independent `sudo -n btrfs filesystem du -s` probes over the broad `/home` tree, selected top-level directories, and even the single live DB file timed out after 18-60 seconds without totals. GNU `du -sxB1 /home/andnasnd/.local/share` reported 766,010,490,880 bytes, including 89,842,393,088 bytes in `.local/share/apollo`, roughly 661 GB of live SQLite main-file allocation and ~2.5 GB WAL. `findmnt -T /home` shows `/dev/nvme0n1p3[/home]` while `/var` is `/dev/nvme0n1p3[/root]`; filesystem-wide `df` also includes that separate root subvolume. Neither a remaining reflink reference nor deferred btrfs extent/discard processing is established as the cause of the missing free-space increase. The other operator's bounded `find / -xdev -size +50G` and `find /home -xdev -size +50G` found no second file over 50 GB besides the live DB. Do not budget 524 GiB as recovered until filesystem-wide usage actually falls.
