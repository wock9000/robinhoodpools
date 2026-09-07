# Robinhood Pools

Robinhood Pools is a standalone observatory for liquidity pools on Robinhood Chain (chain ID `4663`). One Python process reads public RPC data, maintains a local SQLite index, serves JSON/SSE APIs, and serves the plain-HTML/CSS/JavaScript terminal and workbench. It does not require another checkout or any private application files.

The service covers reviewed V2, V3-style, Slipstream, and V4 deployments recorded in `rhpools.lp_chain`. Coverage is deliberately chain-specific: addresses or assumptions from other chains must not be added without review. A V4 manager address identifies the manager, not an individual pool.

## Install and run

Prerequisites:

- Python 3.11 or newer
- [uv](https://docs.astral.sh/uv/)
- [Git](https://git-scm.com/) to clone the public repository

Clone and run the standalone repository:

```sh
git clone https://github.com/wock9000/robinhoodpools.git
cd robinhoodpools
uv sync --locked --extra test
uv run rhpools
```

Open <http://127.0.0.1:8196/>. The default RPC is Robinhood Chain's official public endpoint, and the SQLite index is kept under `~/.local/share/rhpools`. No credential or private companion repository is needed for the default run.

Useful overrides are explicit CLI flags:

```sh
uv run rhpools \
  --host 127.0.0.1 \
  --port 8196 \
  --rpc-url https://rpc.mainnet.chain.robinhood.com \
  --data-dir ~/.local/share/rhpools \
  --database ~/.local/share/rhpools/lp_market.sqlite \
  --history-days 30 \
  --disk-reserve-gib 4
```

Run `uv run rhpools --help` for the installed command's complete option list. `--public-origin` may be repeated to authorize an additional browser origin for protected POST requests. Keep the listener on loopback unless you have separately designed authentication, TLS, proxy limits, and host-level access control.

The equivalent environment settings are `RHP_HTTP_HOST`, `RHP_HTTP_PORT`, `RHP_RPC_URL`, `RHP_DATA_DIR`, `RHP_DATABASE`, `LP_HISTORY_DAYS`, and `LP_DISK_RESERVE_GIB`. Additional origins and transaction preparation require explicit CLI flags; there is no environment switch that silently enables preparation.

### Serving health is not index freshness

A successful request to `/` establishes only that the HTTP process can serve the installed UI. Index progress and source coverage are separate; inspect `/api/lp/status` and its observed head, indexed head, lag, history coverage, and provider status before treating results as current. During startup, catch-up, provider failure, or a chain reorganization, the server can remain available while indexed data is incomplete or stale.

```sh
curl -fsS http://127.0.0.1:8196/ >/dev/null
curl -fsS http://127.0.0.1:8196/api/lp/status
```

## Safety model

The default service observes and simulates; it never signs or broadcasts a transaction. Transaction preparation is disabled unless `--enable-transaction-prepare` is supplied. Enabling it permits generation of unsigned transaction data for independent inspection and external signing; it does not enable server signing or broadcasting. Never give this process, its browser UI, configuration, or repository a seed phrase or private key.

RPC credentials, when needed, belong only in local mode-`0600` files containing one URL per line. Point a capability-specific variable such as `LP_RPC_STATE_URL_FILES` or `LP_RPC_TRACE_URL_FILES` at the file; `LP_RPC_HEAD_URL_FILES`, `LP_RPC_HISTORY_STATE_URL_FILES`, `LP_RPC_LOG_URL_FILES`, and `LP_RPC_RECEIPT_URL_FILES` follow the same convention. Generic runtime RPC files use `RHP_RPC_URL_FILES`. Do not put credential-bearing URLs in CLI arguments, browser storage, committed environment files, fixtures, screenshots, or logs.

One running `rhpools` process owns a database. Do not point concurrent processes at the same SQLite file, inspect it with write-capable tools while the service is running, or use a live database in tests. Stop the owner before backup or migration work and copy the database together with its SQLite sidecar files when they exist.

## Data interpretation

Results represent the service's indexed evidence and declared coverage, not guaranteed global truth. Unknown accounting stays `null`; consumers must not turn missing evidence into zero. USDG-denominated values are quote values, not an assertion that USDG equals fiat USD. Units and partial-history coverage are part of the API contract and must remain visible to consumers.

The versioned public API is described in [`docs/PUBLIC_API.md`](docs/PUBLIC_API.md), with the machine-readable schema served at `/api/v1/openapi.json`. The browser also uses `/api/lp/*` and `/api/workbench/*` routes; these remain same-process client routes rather than a reason to expose RPC credentials to JavaScript.

## Repository map

- `src/rhpools/lp_server.py` — installed `rhpools` CLI, process lifecycle, HTTP, static assets, and route boundaries
- `src/rhpools/lp_chain.py` — side-effect-free chain ID and reviewed public deployment registry
- `src/rhpools/lp_rpc.py` — runtime RPC source selection and capability handling
- `src/rhpools/lp_market_protocols.py` — protocol decoding and pool identity rules
- `src/rhpools/lp_market_index.py` — chain ingestion, catch-up, live activity, and reorganization handling
- `src/rhpools/lp_market_store.py` — SQLite schema, migrations, cursors, and stored queries
- `src/rhpools/lp_market_accounting.py` — derived position and episode accounting
- `src/rhpools/lp_market_service.py` — indexed market query orchestration
- `src/rhpools/lp_public_api.py` — versioned public pool and asset API
- `src/rhpools/lp_allocation.py` — read-only allocation previews
- `src/rhpools/workbench_market.py` and `workbench_actions.py` — pool detail, simulation, and optional unsigned preparation
- `src/rhpools/lp_research.py` and `lp_fomo_flow.py` — research and public flow views
- `src/rhpools/static/` — framework-free browser UI
- `tests/` — deterministic pytest coverage
- `docs/` — public API, provenance, RPC operations, and research semantics

See [`CONTRIBUTING.md`](CONTRIBUTING.md) before changing contracts or persisted data.

## License

Copyright 2026 RobinhoodPools contributors. Licensed under **AGPL-3.0-only**; see [LICENSE](LICENSE) and [third-party notices](THIRD_PARTY_NOTICES.txt). The software is provided without warranty.

If you modify the application and let users interact with it over a network, offer those users the corresponding source of your modified version as required by section 13. Preserve the visible source links and update them to your fork when deploying a modified version.

The public source is <https://github.com/wock9000/robinhoodpools>. Software licensing does not grant rights to external data services, trademarks, credentials, or unrelated private applications. This is independent community tooling, not an official Robinhood or Uniswap product.
