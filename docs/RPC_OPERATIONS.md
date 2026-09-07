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

## Goldsky measurements and limits

A small anonymous-output probe from the production host verified the donated provider privately: chain 4663; exact matching block/header and log digests against the local node; old block headers; USDG `decimals()` at blocks 30,000,000 and 56,400,000; receipts; `debug_traceTransaction` with `callTracer`; and a four-item JSON-RPC batch. The local pruned node could not answer those archive-state calls. Individual successful Goldsky requests in this probe took roughly 76–352 ms; these are samples, not percentile/SLA claims.

Goldsky supports HTTPS JSON-RPC, not WSS subscriptions. Keep the existing independent WSS head/activity source. Route cheap local headers/logs locally and use Goldsky for archive state, receipts/traces, current state, and fallback. Enrichment remains bounded; enabling archive access does not instantly complete historical ownership or P/L.

The donated allowance is 6,000 requests/minute. This process paces Goldsky at at most 80 JSON-RPC items/second on average per endpoint, counting batch elements conservatively, with four concurrent HTTP requests. Bursts are bounded by the batch size (100). This leaves nominal headroom under 100/s but does not account for other applications sharing the key. Provider billing/rate accounting remains authoritative.

`/api/lp/status` includes per-source `traffic`: total HTTP attempts, total JSON-RPC items, and rolling approximately 60-second rates. Failed attempts and chain verification count. Traffic is endpoint-wide and repeated under capabilities using that endpoint: **do not sum repeated capability rows**. WSS messages and explorer fallback GETs are not included in these HTTP JSON-RPC counters.

Goldsky primary references:

- <https://docs.goldsky.com/edge-rpc/introduction.md>
- <https://docs.goldsky.com/edge-rpc/platform/security.md>

## Indexer bottlenecks

Separate durable cursor progress from the live observed feed. A moving live tape is not evidence of complete historical financial accounting. Compare cursor gain and chain gain over the same interval; recent-gap-first scheduling intentionally pauses older backfill while the recent gap is large.

Production sampling found wallet aggregation repeatedly restarting whenever ingestion advanced its revision. This could keep LP Wallets at `SYNCING` indefinitely while consuming CPU needed by the indexer. Wallet totals, gas and coverage now come from one completed WAL read snapshot; appends trigger a later refresh instead of recursive recomputation. Canonical-branch changes still invalidate service publications. The regression exercises an indexer append during an actual wallet read.

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
