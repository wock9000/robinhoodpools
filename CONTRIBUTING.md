# Contributing

Work from this repository alone. Changes must not depend on private checkouts, production databases, unpublished modules, or developer-specific paths.

## Development setup

Python 3.11+ and uv are the only Python tooling prerequisites:

```sh
uv sync --locked --extra test
uv run pytest
```

Run the application from the same environment:

```sh
uv run rhpools
```

The default test suite must be deterministic and offline. Tests must not contact a live RPC, production service, or public internet endpoint and must not require RPC environment variables. Use existing fake transports, temporary directories, and temporary SQLite databases. If a test is specifically an opt-in integration probe, keep it outside the default suite and document its isolation in the change description.

## Change boundaries

Keep the project standalone and keep responsibilities in their existing modules:

- chain constants and reviewed public deployments: `lp_chain.py`
- RPC capability selection: `lp_rpc.py`
- protocol decoding and identities: `lp_market_protocols.py`
- ingestion and reorganization behavior: `lp_market_index.py`
- persisted schema, migrations, and queries: `lp_market_store.py`
- derived accounting: `lp_market_accounting.py`
- HTTP and process lifecycle: `lp_server.py`
- versioned consumer API: `lp_public_api.py`
- browser UI: `static/`

Do not introduce a second convention beside one already used. Do not move secrets, operator policy, wallet identity, or runtime configuration into source constants. Never import code from another checkout. The browser must call this server, not an RPC provider directly.

A single service process owns each SQLite database. Development scripts and tests must use a fresh temporary database, never the database used by a running service. Stop the owner before copying or inspecting persisted state with a write-capable tool.

## Tests and regressions

A regression test should fail for a plausible consumer-visible bug and should cover behavior, a boundary, invariant, transition, precedence rule, or real error. Avoid assertions about field forwarding, internal calls, source text, incidental wording, or merely not raising.

For RPC/index changes, use deterministic responses that exercise the actual decoder, cursor, retry, or reorganization boundary without network access. For API changes, assert the response a consumer observes and preserve these invariants:

- chain scope is Robinhood Chain ID `4663`;
- every numeric quantity has an unambiguous unit;
- unknown accounting remains `null`, never an invented zero;
- USDG quote values are not represented as guaranteed fiat value;
- requested, available, and partial-history coverage remain distinguishable;
- pool identity is protocol-correct, including that a V4 manager is not a pool;
- pagination/cursor ordering remains deterministic;
- stale or incomplete index state is distinguishable from HTTP serving availability.

A persisted-schema change must be implemented as a forward migration in `lp_market_store.py`. Prove that a database at the previous schema opens and migrates without losing indexed events, cursors, accounting state, or coverage metadata, and that a new empty database reaches the same schema. Do not rewrite historical migration meaning or require contributors to delete their data directory.

Run the focused test while developing, then the complete suite before submitting:

```sh
uv run pytest tests/test_lp_market_store.py
uv run pytest
```

Choose the focused file that owns the changed contract; the store test above is only an example.

## Browser changes

The UI is plain HTML, CSS, and JavaScript. Keep it framework-free and do not add a package-manager build solely for syntax or asset delivery. Check changed JavaScript with Node's parser when Node is available:

```sh
node --check src/rhpools/static/lp_terminal.js
```

A UI change is not verified by unit tests alone. Run `uv run rhpools`, open the actual affected route in a browser, exercise the changed interaction, and inspect the rendered result. Include relevant loading, empty, stale/error, keyboard, and narrow-viewport behavior when the change touches those states. Confirm that freshness indicators describe index state rather than merely HTTP success.

## Credentials and local data

Never commit private keys, seed phrases, credential-bearing RPC URLs, `.env` files, local databases or SQLite sidecars, captured production responses, browser storage, or screenshots/logs containing secrets. Never paste a key into the CLI or browser.

For a credentialed RPC, create a local file with exactly one URL per line and restrict it before starting the service. Enter the URL in a local editor so it does not appear in shell history:

```sh
install -d -m 700 "$HOME/.config/robinhoodpools"
install -m 600 /dev/null "$HOME/.config/robinhoodpools/state-rpc.urls"
${EDITOR:-vi} "$HOME/.config/robinhoodpools/state-rpc.urls"
chmod 600 "$HOME/.config/robinhoodpools/state-rpc.urls"
export LP_RPC_STATE_URL_FILES="$HOME/.config/robinhoodpools/state-rpc.urls"
uv run rhpools
```

Use the matching `LP_RPC_*_URL_FILES` variable for head, history-state, log, receipt, or trace capability files. Use `RHP_RPC_URL_FILES` only for a generic runtime source. Keep credential files outside the repository; never submit their contents in an issue or review comment.

## Pull requests

Keep changes narrowly owned and explain the observable contract. State which focused scenario and complete test command passed; for UI work, state the route and browser interaction exercised. Call out API compatibility, migration/data compatibility, units, coverage semantics, and security consequences where applicable.

Do not add deployment jobs, production endpoints, repository secrets, or transaction signing as part of an unrelated change. Transaction preparation, when explicitly enabled, must remain unsigned and non-broadcasting.

Contributions are made under AGPL-3.0-only. Preserve third-party notices and only contribute code you own or have permission to redistribute under compatible terms. A modified hosted version must offer its corresponding source to users; point the visible source links at the repository containing that version.
