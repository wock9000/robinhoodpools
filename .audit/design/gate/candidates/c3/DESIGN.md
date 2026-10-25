# Gate design, candidate 3: one credential, wallet-level entitlement

Scope: TOKEN_PLAN units 0-2. Policy, holding oracle, sign-in, keys, gated REST/SSE/WS, rate limits, cookie/CORS rules, server integration, header UI, test harness.

## Problem

rhpools is a stdlib `ThreadingHTTPServer` with no identity of any kind: no cookies, no keys, `ACAO: *` on every JSON body, a body memo cache keyed by publication revision, and a cloudflared path allowlist. It must grow holder-gated features (trade, lp, api, flags) without changing a byte of any anonymous response, without touching the 652 GB market SQLite or its writer, and without ever holding a key. The owner is pinned in host config and changes policy only by signature. The token does not exist yet; the gate must run with the token unset (everything gated is refused) and with a stand-in ERC-20 on an anvil fork.

The shape that falls out: identity is a wallet, proven once by SIWE; the proof is exchanged for one opaque secret, the *key*. The browser keeps its key in an HttpOnly cookie; programs send the same kind of key as a bearer. Entitlement is a property of the wallet (balance vs. thresholds, with grace), never of the key. One function turns request headers into `(Principal, Entitlement)` and every gated route, stream, and socket calls that function and nothing else.

Constraints honored from grounding (lp_server.py, workbench_actions.py, deploy/, SECURITY.md):

- `_json` memoizes bodies by publication key and marks them `public`; a gated variant of an existing payload must never share that memo slot or that cache header.
- `Handler` has only `do_GET/POST/OPTIONS/HEAD`; new routes are added to `_ROUTES`/explicit path sets and to the tunnel regex.
- POSTs already require `_same_origin()`; simulate/prepare additionally require loopback. Those checks stay exactly as they are.
- Wallet code is raw EIP-1193 (`workbench.js`); no ethers/viem, and CSP forbids vendored-by-CDN. SIWE needs only `personal_sign`.
- Market RPC is lane-managed and contended; the oracle gets its own tiny JSON-RPC client to the local Nitro node.

## Usage (caller's view)

### Browser (terminal header)

```
click WALLET → eth_requestAccounts → GET /api/gate/nonce
          → personal_sign(SIWE message) → POST /api/gate/session {message, signature}
          ← 200 {wallet, features:["api","flags"], holding:{...}, expires_at}  + Set-Cookie: rhp_key=…; HttpOnly; Secure; SameSite=Strict
page load → GET /api/gate/me  (cookie rides along)      ← same body, or {signed_in:false}
```

Nothing else in the terminal changes. `fetch` and `EventSource` are same-origin, so the cookie is sent automatically on every existing route; routes that know about gating (tape, stream; unit 3) add fields when the wallet is entitled and stay byte-identical otherwise.

### Programmatic client

```sh
# 1. sign in once from any wallet (cast shown; browser works too)
curl -s https://rhpools.lol/api/gate/nonce
cast wallet sign --private-key $PK "$SIWE_MESSAGE"
curl -s -X POST https://rhpools.lol/api/gate/session -H 'content-type: application/json' \
     -d '{"message":"…","signature":"0x…","label":"research-box"}' -c cookies
# 2. mint a long-lived key from the signed-in session (cookie or existing key)
curl -s -b cookies -H 'origin: https://rhpools.lol' -X POST https://rhpools.lol/api/gate/keys \
     -H 'content-type: application/json' -d '{"op":"mint","label":"bot-1","ttl_s":7776000}'
# → {"key_id":"5c1d9b2e0a7f43aa","secret":"rhp_Qm…(shown once)","expires_at":…}
# 3. use it
curl -s -H 'authorization: Bearer rhp_Qm…' https://rhpools.lol/api/lp/tape?kind=lp
curl -N -H 'authorization: Bearer rhp_Qm…' https://rhpools.lol/api/v1/stream?channel=activity
websocat -H 'authorization: Bearer rhp_Qm…' wss://rhpools.lol/api/v1/ws?channel=activity
```

Every gated refusal is JSON: `401 {"error":"credential required","gate":{"state":"anonymous"}}`, `403 {"error":"not entitled","gate":{"state":"below","feature":"api","need":"1000","have":"12.5","grace_until":null}}`, `429 {"error":"key rate limit","gate":{"retry_after_ms":180}}`. A stream that loses entitlement gets `event: gate\ndata: {"state":"revoked",…}` and a clean close.

### Owner

```sh
rhpools-gate policy show
rhpools-gate policy typed-data policy.json > td.json        # EIP-712 JSON for the wallet
cast wallet sign --data --from-file td.json --ledger        # or MetaMask via the terminal's owner panel
rhpools-gate policy apply policy.json --signature 0x…       # verified against RHP_GATE_OWNER, audit row
rhpools-gate audit --limit 20
rhpools-gate keys revoke --key-id 5c1d9b2e0a7f43aa
```

The web route `POST /api/gate/policy` takes the identical `{policy, signature}` body; the CLI is the fallback transport, not a second authority.

### Inside lp_server.py (the three call sites)

```python
# any gated REST route
principal, ent = self.runtime.gate.require(self.headers, "api")   # raises GateRefusal
self.runtime.gate.admit(principal)                                 # per-key token bucket, raises GateRefusal(429)

# existing route that grows fields for entitled wallets (unit 3 wiring point)
principal = self.runtime.gate.resolve(self.headers)                # None for anonymous: untouched path
ent = self.runtime.gate.entitlement(principal.wallet) if principal else None
payload = method(query)                                            # unchanged
if ent is not None and ent.has("flags"):
    payload = self.runtime.flags.decorate(payload)                 # unit 3
    return self._json(200, payload)                                # no memo key, no public cache

# stream loop, once per iteration (cheap: oracle cache answers inside TTL)
ent = self.runtime.gate.recheck(principal, "api")                  # raises GateRefusal when key revoked/expired or grace ended
```

## Shape

### Data (gate.py)

```python
Feature = Literal["trade", "lp", "api", "flags"]
FEATURES: tuple[Feature, ...] = ("trade", "lp", "api", "flags")

@dataclass(frozen=True)
class GatePolicy:
    """The owner-signed policy. `token is None` means unset: every feature refuses."""
    version: int                   # strictly increasing; replay guard
    token: str | None              # checksum address or None
    decimals: int
    threshold: dict[Feature, int]  # raw units; 0 = any signed-in wallet
    grace_s: int
    issued_at: int                 # unix s, must be within ±600 s of apply time

    def typed_data(self, chain_id: int = 4663) -> dict: raise NotImplementedError   # EIP-712 JSON (domain "rhpools gate"/"1"/4663, primaryType GatePolicy)
    def digest(self, chain_id: int = 4663) -> bytes: raise NotImplementedError      # keccak(0x1901 ‖ domainSep ‖ structHash); verified against `cast wallet sign --data`

@dataclass(frozen=True)
class Holding:
    wallet: str; balance_raw: int; block: int; observed_at: float

@dataclass(frozen=True)
class Entitlement:
    wallet: str
    features: frozenset[Feature]
    holding: Holding | None            # None when token unset or oracle failed and nothing cached
    grace_until: dict[Feature, float]  # for features held only by grace
    policy_version: int
    def has(self, feature: Feature) -> bool: raise NotImplementedError
    def public(self) -> dict: raise NotImplementedError   # wire shape for /me and refusals; never includes key material

@dataclass(frozen=True)
class Principal:
    wallet: str; key_id: str; label: str; via: Literal["cookie", "bearer"]; expires_at: int

@dataclass(frozen=True)
class KeyRecord:
    key_id: str; wallet: str; label: str; created_at: int; expires_at: int; revoked_at: int | None; last_used_at: int | None

class GateRefusal(Exception):
    status: int          # 401 anonymous, 403 not entitled/forbidden, 429 rate limit
    payload: dict        # {"error": ..., "gate": {...}}
```

The key secret is `rhp_` + base64url(32 random bytes). Only `sha256(secret)` is stored; `key_id` is the first 8 bytes of that hash in hex and is the public handle. There is one `credential` table and one Principal type. `via` records how it arrived (cookie/bearer) because the CSRF rule needs it; it is not a kind.

### Gate: one object, one path

```python
class Gate:
    """Credential → wallet → entitlement for every request, stream and socket.

    Owns gate.sqlite (WAL, busy_timeout 5 s) and never touches the market store.
    All public methods are thread-safe. `clock` is injectable for tests.
    """
    def __init__(self, db_path: Path, *, owner: str | None, rpc_url: str, hosts: frozenset[str],
                 chain_id: int = 4663, limits: Limits = Limits(), clock=time.time) -> None: raise NotImplementedError

    # --- the one path ------------------------------------------------------
    def resolve(self, headers: Message) -> Principal | None:
        """Bearer wins over cookie. Unknown/expired/revoked key → None (anonymous), never an error.
        Touches last_used_at at most once per 60 s per key."""
        raise NotImplementedError
    def entitlement(self, wallet: str) -> Entitlement:
        """policy + oracle + grace. Pure given (policy, holding, qualified rows, now)."""
        raise NotImplementedError
    def require(self, headers: Message, feature: Feature) -> tuple[Principal, Entitlement]:
        """resolve → entitlement → GateRefusal(401|403) unless ent.has(feature)."""
        raise NotImplementedError
    def recheck(self, principal: Principal, feature: Feature) -> Entitlement:
        """Stream re-validation: key still live (not revoked/expired) and ent.has(feature); else GateRefusal."""
        raise NotImplementedError
    def admit(self, principal: Principal, cost: int = 1) -> None:
        """Token bucket per key_id (limits.key_rps, limits.key_burst); raises GateRefusal(429)."""
        raise NotImplementedError
    def stream_slot(self, principal: Principal) -> AbstractContextManager[None]:
        """Bounded concurrent streams per wallet (limits.streams_per_wallet); raises GateRefusal(429)."""
        raise NotImplementedError

    # --- sign-in and keys ----------------------------------------------------
    def nonce(self) -> dict:                                  # {"nonce","issued_at","expires_at","domain","chain_id","statement"}
        raise NotImplementedError
    def sign_in(self, message: str, signature: str, *, label: str) -> tuple[Principal, str]:
        """Parses EIP-4361, checks domain ∈ hosts, chain 4663, nonce issued+unused (single use), not expired,
        recovers signer (EIP-191 ecrecover; ERC-1271 eth_call only when code exists and is not an EIP-7702
        designator 0xef0100‖addr). Mints a key with limits.session_ttl_s. Returns (principal, secret)."""
        raise NotImplementedError
    def mint_key(self, principal: Principal, *, label: str, ttl_s: int) -> tuple[Principal, str]:
        """Requires entitlement 'api'; ≤ limits.keys_per_wallet live keys. Secret returned once."""
        raise NotImplementedError
    def keys(self, wallet: str) -> list[KeyRecord]: raise NotImplementedError
    def revoke(self, wallet: str, key_id: str) -> bool: raise NotImplementedError   # idempotent; only own keys
    def revoke_all(self, wallet: str) -> int: raise NotImplementedError

    # --- policy ----------------------------------------------------------------
    def policy(self) -> GatePolicy: raise NotImplementedError                     # cached; unset policy when no row
    def apply_policy(self, policy: GatePolicy, signature: str, *, via: Literal["web", "cli"]) -> GatePolicy:
        """Recover signer from policy.digest(); refuse unless signer == owner (case-insensitive), version > current,
        |issued_at − now| ≤ 600. Insert policy row + audit row in one transaction. Idempotent on the same
        (version, signature): returns current without a second audit row."""
        raise NotImplementedError
    def audit(self, limit: int = 50) -> list[dict]: raise NotImplementedError
    def status(self) -> dict: raise NotImplementedError      # for /api/lp/status is NOT touched; exposed on /api/gate/policy
    def close(self) -> None: raise NotImplementedError
```

`Limits` is operational, not signed: `session_ttl_s=7d, key_ttl_max_s=365d, keys_per_wallet=8, streams_per_wallet=4, key_rps=10, key_burst=40, nonce_ttl_s=600`. It comes from CLI flags with defaults; the owner changes it by editing the unit file, like the origins today.

### Holding oracle (inside gate.py; not public)

```python
class _Oracle:
    """balanceOf(wallet) at `latest` via a private JSON-RPC session to rpc_url (default http://127.0.0.1:8547).
    30 s per-wallet cache, single-flight per wallet, bounded to 4096 wallets (LRU).
    On RPC failure: return cached Holding if present (any age), else None → fail closed."""
    def observe(self, token: str, wallet: str) -> Holding | None: raise NotImplementedError
```

Entitlement rule, evaluated per feature with `now`:

```
qualified_now  = holding is not None and holding.balance_raw >= threshold[f]
if qualified_now: qualified[wallet, f].last_ok_at = now       (persisted; write only when it moves ≥ 5 s)
entitled(f)    = qualified_now or (now - qualified[wallet, f].last_ok_at) <= grace_s
grace_until[f] = last_ok_at + grace_s  when entitled by grace only
```

Threshold 0 means "any signed-in wallet" (used for `flags` if the owner wants sign-in-only). Token unset ⇒ `qualified_now` is False for every feature and grace rows are ignored, so nobody is entitled. One rule covers the holder's own sell, price flaps, and RPC blips; the grace clock is the *last observed qualifying balance*, so a wallet that dips and recovers inside the window never notices.

### Credential rules

- Precedence: `Authorization: Bearer rhp_…` if present, else cookie `rhp_key`. A request carrying both uses the bearer; the cookie is ignored (never combined).
- Cookie: `rhp_key=<secret>; Path=/; HttpOnly; Secure; SameSite=Strict; Max-Age=<session_ttl_s>`. Set only by `POST /api/gate/session`; cleared by `POST /api/gate/logout` (which also revokes that key). Strict is enough: the document is anonymous, every fetch/EventSource/WebSocket from it is same-site.
- CSRF: a cookie-authenticated POST must pass the existing `_same_origin()`; a bearer-authenticated POST need not (a bearer cannot be attached by a cross-site form). GETs are never state-changing.
- CORS: `ACAO: *` stays on every response. Cookie responses cannot be read cross-origin under `ACAO: *` (browsers refuse `*` with credentials), so gated cookie data cannot leak to another origin. `OPTIONS` for `/api/gate/*`, `/api/v1/stream`, and any gated GET adds `Authorization` to `Access-Control-Allow-Headers` so browser bots on other origins can use a bearer.
- Caching: any response computed for a principal is sent with `Cache-Control: private, no-store`, no ETag, no memo key, `Vary: Authorization, Cookie`. Anonymous responses go through the untouched `_json(key=…, fresh=…)` path.
- Secrets never appear in query strings, logs (`log_message` already strips queries; refusals log `key_id` only), or `/api/gate/keys` after mint.
- Loss of the key file: none exists. Loss of gate.sqlite: everyone is signed out and the policy is unset until re-applied; the audit log is the loss.

### Transport: SSE and WebSocket for keyed clients

`GET /api/v1/stream` is `_lp_stream` behind `require(…, "api")` and `stream_slot`, with two additions in the loop: `recheck` every iteration (the oracle cache makes it a dict lookup inside the TTL; a wallet's balance is fetched at most every 30 s regardless of how many streams it holds) and, on `GateRefusal`, `event: gate` followed by close. `GET /api/v1/ws` upgrades (RFC 6455, `lp_ws.py`: handshake, server→client text frames, ping/pong, close; client frames other than pong/close are answered with close 1003) and pumps the same frames as JSON text messages: `{"event":"activity","id":"…","data":{…}}`. Browsers connect to `/api/v1/ws` with the cookie, programs with the header; `resolve` does not care which. The anonymous `/api/lp/stream` and `/api/workbench/stream` are not touched.

Stream capacity: keyed streams use a third semaphore `keyed_streams` (128) so they neither consume nor are starved by the 512 anonymous slots; per wallet, `streams_per_wallet`.

### Server integration (lp_server.py)

```python
# new routes
_GATE_GET  = {"/api/gate/nonce", "/api/gate/me", "/api/gate/keys", "/api/gate/policy"}
_GATE_POST = {"/api/gate/session", "/api/gate/keys", "/api/gate/logout", "/api/gate/policy"}
_KEYED_STREAMS = {"/api/v1/stream": "sse", "/api/v1/ws": "ws"}
_LANE_SHARE = {"fast": 1.0, "slow": 0.5, "keyed": 0.5}       # keyed REST gets its own lane

class Runtime:  # gains
    self.gate = Gate(args.gate_db, owner=args.gate_owner, rpc_url=args.gate_rpc_url, hosts=hosts_of(self.origins), limits=Limits.from_args(args))

class Handler:  # gains
    keyed_streams = threading.BoundedSemaphore(128)
    def _gate_get(self, path, query): raise NotImplementedError     # nonce/me/keys/policy; me and keys need resolve()
    def _gate_post(self, path, payload): raise NotImplementedError  # session/keys/logout/policy; Set-Cookie on session/logout
    def _keyed_stream(self, kind, query): raise NotImplementedError # require → admit → stream_slot → _lp_stream/_ws_pump with recheck
    def _refuse(self, refusal: GateRefusal): raise NotImplementedError  # _json(refusal.status, refusal.payload, retry=…); private no-store
```

`do_GET` gains two branches before the 404: `path in _GATE_GET` and `path in _KEYED_STREAMS`. `do_POST` gains `path in _GATE_POST`, evaluated *after* the existing `_same_origin()` check only when the credential is a cookie (bearer POSTs skip it), keeping the three legacy POST paths untouched. `_json` gains one keyword, `private: bool = False`, which forces `no-store`, skips ETag/memo, and adds `Vary`. Policy/owner: `--gate-owner` (env `RHP_GATE_OWNER`), `--gate-db` (default `data_dir/gate.sqlite`), `--gate-rpc-url` (default `http://127.0.0.1:8547`). Systemd unit adds `Environment=RHP_GATE_OWNER=0x…`. Tunnel regex adds `/api/gate/(nonce|session|me|keys|logout|policy)`, `/api/v1/(stream|ws)`, `/static/lp_gate\.(css|js)`.

`/api/workbench/capabilities` is unchanged; a new field is not needed because `/api/gate/me` is the capability surface for signed-in users.

### Owner CLI (gate_cli.py, script `rhpools-gate`)

Opens the same `Gate` on the same file (SQLite handles the two processes; WAL + busy_timeout). Commands: `policy show|typed-data|apply`, `audit`, `keys list|revoke`. `apply` runs the identical `Gate.apply_policy(..., via="cli")`. There is no unsigned override: the fallback is for when the web path is down, and the signature is produced with `cast wallet sign --data --from-file` (verified to match the Python digest) or a hardware wallet.

Audit row: `(id, at, actor, via, action, detail_json)` with actions `policy.apply`, `policy.refused`, `key.mint`, `key.revoke`, `key.revoke_all`, `session.sign_in`, `session.refused`. Refusals are audited with the recovered signer so a stolen-signature attempt is visible.

### Header UI (static/lp_gate.js + lp_gate.css, terminal header)

A single chip `#gate-chip` in the status strip after `#strip-freshness`, and a `<dialog id="gate-dialog">` for keys/policy. States, all driven by one `render(state)` from `/api/gate/me` and wallet events:

| state | chip text | click |
|---|---|---|
| `no-wallet` | `WALLET · none` (dim) | opens guide anchor |
| `disconnected` | `WALLET · connect` | `eth_requestAccounts` |
| `connected` | `0xab…cd · sign in` | nonce → `personal_sign` → session |
| `signing` | `0xab…cd · confirm in wallet…` | — |
| `holder` | `0xab…cd · HOLDER trade lp api flags` (green) | dialog: holding, keys |
| `grace` | `0xab…cd · GRACE 12:31` (yellow, ticking) | dialog |
| `below` | `0xab…cd · need 1,000 RHP (have 12.5)` (red) | dialog |
| `unset` | `0xab…cd · gate not live` (dim) | dialog |
| `mismatch` | `0xef…01 ≠ session · sign in` | sign in again |
| `owner` | adds `· POLICY` link to the dialog's policy tab (`eth_signTypedData_v4` on the same typed data) | |

No wallet library: `window.ethereum.request` for `eth_requestAccounts`, `eth_chainId`, `wallet_switchEthereumChain`, `personal_sign`, `eth_signTypedData_v4`. Chain check mirrors `workbench.js` `ensureChain`. The SIWE message is built client-side from the nonce response (domain, uri, chainId, nonce, issuedAt, expirationTime, statement) and echoed verbatim to the server, which parses and validates it. Keys tab shows `key_id · label · expires · last used · REVOKE`, and `MINT KEY` shows the secret once inside a `<code>` with copy.

### Invariants (where they live)

- Server never signs or holds keys: `Gate` has no signing code path; `eth-keys` is used for recovery only (type-level: only `Signature.recover_public_key_from_msg_hash` is imported). Test asserts `PrivateKey` is not referenced outside tests.
- Anonymous byte-identity: `resolve()` is the only new work on the anonymous path and returns `None` without touching the DB when neither header is present; golden test compares bodies and cache headers with and without a `Gate` installed.
- No market-writer contact: `Gate` receives no store reference; import graph test asserts `gate.py` does not import `lp_market_*`.
- Owner-only policy: `apply_policy` is the only writer of `policy`; both transports call it; non-owner signer → `GateRefusal(403)` + audit row.
- Fail closed: token unset, oracle miss with no cache, unknown key, expired nonce, wrong domain/chain → refused; never "allow because unsure".
- Wallet-level entitlement: `entitlement(wallet)` takes a wallet, not a key; revoking a key never changes another key's outcome; losing entitlement affects every key at the next `recheck`.
- Idempotence: `apply_policy` on the same `(version, signature)` is a no-op; `revoke` twice is true then false with one audit row; nonce use is a single `UPDATE … WHERE used_at IS NULL` row-count check.

### What it deliberately does not do

No per-key feature scopes (wallet-level is the direction). No refresh tokens (session keys are long-lived and revocable). No admin UI for limits. No gating of HTML pages. No WebSocket client→server commands beyond pong/close. No ERC-1271 for the owner (owner is an EOA pinned in host config).

## Module map

| file | owns | status |
|---|---|---|
| `src/rhpools/gate.py` | GatePolicy/EIP-712, SIWE parse+verify, Oracle, credential store, entitlement, limits, audit; `Gate` | new |
| `src/rhpools/gate_cli.py` | `rhpools-gate` argparse over `Gate` | new |
| `src/rhpools/lp_ws.py` | RFC 6455 server-push framing on the handler socket | new |
| `src/rhpools/lp_server.py` | routes, `_json(private=)`, keyed lane/streams, cookie headers, refusal mapping | edit |
| `src/rhpools/static/lp_gate.js`, `lp_gate.css` | header chip, SIWE flow, keys/policy dialog | new (+`_ASSETS`, terminal html `<script>`) |
| `deploy/robinhoodpools.service`, `robinhoodpools-tunnel.json` | `RHP_GATE_OWNER`, allowlist | edit |
| `docs/PUBLIC_API.md`, `static/openapi.json`, `SECURITY.md` | "no credentials" statements become "anonymous routes accept none; keyed routes accept a bearer" | edit |
| `pyproject.toml` | `eth-keys>=0.6,<1` (pure-Python backend; no native build), script `rhpools-gate` | edit |

Call chain for a gated request: `Handler.do_GET → gate.require → (resolve, entitlement→_Oracle.observe)`: two files.

## Test and verification harness

`tests/test_gate.py` (fast, no network; `Gate(rpc_url=…)` with `_Oracle` replaced by a fake returning scripted balances, `clock` injected):

- policy: owner signature accepted; non-owner refused with audit row; version ≤ current refused; issued_at skew refused; same (version, signature) twice → one row; `typed_data()` digest equals a fixture signed by `cast wallet sign --data` (fixture signature checked in).
- SIWE: domain not in hosts, wrong chain, reused nonce, expired nonce, expired message, bad signature, signature from a different address, 7702-delegated EOA (code `0xef0100…`) verified by ecrecover, contract wallet path via fake ERC-1271 responder.
- credential: cookie vs bearer precedence; unknown/revoked/expired → anonymous; `keys_per_wallet` cap; revoke idempotence; secret never stored; `key_id` derivation.
- entitlement transitions: below→holder→grace→below with a scripted balance and clock; threshold 0; token unset; oracle failure with stale cache inside/outside grace; per-feature thresholds.
- limits: token bucket refill; `streams_per_wallet` context manager.

`tests/test_lp_server.py` additions (existing `serving()` fixture plus `gate=`):

- golden: every route in `_ROUTES`, `/api/lp/stream` first frames, `/api/workbench/capabilities`, and OPTIONS answer byte-identically with and without `gate` installed and with a garbage cookie/bearer.
- `POST /api/gate/session` sets the exact cookie attributes; `/me` via cookie and via bearer; cookie POST without Origin → 403; bearer POST without Origin → 200; gated GET with entitled cookie carries `private, no-store` and no ETag.
- `/api/v1/stream` with bearer: frames flow; scripted balance drop + clock past grace → `event: gate` then EOF; `/api/v1/ws`: handshake, one frame, close on revoke.

`tests/test_gate_fork.py` (skipped unless `~/.foundry/bin/anvil` exists and `RHP_TEST_FORK_RPC`, default `http://127.0.0.1:8547`, answers `eth_chainId` 0x1237):

- fixture starts `anvil --fork-url $RPC --port <free> --chain-id 4663 --silent`, generates *fresh* keys with `eth_keys.PrivateKey(os.urandom(32))` (the well-known anvil accounts carry EIP-7702 delegation on chain 4663 and must not be used), `anvil_setBalance` for gas, and funds the stand-in ERC-20 by impersonating the WETH/USDG pool `0x52e6…71ca` (`anvil_setBalance` + `anvil_impersonateAccount` + `transfer`); `anvil_dealERC20` does not work on USDG (ERC-1967 proxy, slot search fails). Kills anvil in teardown.
- end to end: owner applies policy (token=USDG, thresholds, grace 30 s); holder signs in (SIWE signed in Python with the fresh key), gets features, mints a key, opens `/api/v1/stream`; non-holder is refused; holder transfers below threshold → still entitled until `clock` passes grace → stream closes with `event: gate`, `/me` shows `below`.

Browser proof (manual, documented in the PR): MetaMask on 4663 against the loopback server with `--public-origin http://127.0.0.1:8196`, showing chip states connect → sign in → holder/below, cookie flags in devtools, keys dialog mint/revoke, owner policy signing via `eth_signTypedData_v4`.

## Chain and tooling facts relied on

| fact | status |
|---|---|
| Local Nitro RPC `http://127.0.0.1:8547` answers chain 4663, head ~74.14M | verified (`cast chain-id`, `cast block-number`) |
| `anvil 1.7.1` forks it; `anvil_impersonateAccount` + `transfer` funds a test wallet with USDG; balanceOf readable at a pinned block | verified on a throwaway fork |
| `anvil_dealERC20` fails on USDG ("no slot found"); USDG is an ERC-1967 proxy (impl `0x6818…6f8f`) | verified |
| USDG `decimals()` = 6 (apollo DECISIONS.md's 18 is wrong) | verified (`cast call`) |
| Well-known anvil accounts #0-#2 have EIP-7702 delegation code `0xef01008a67…408a` on chain 4663 | verified (`cast code`) |
| `eth-keys` (pure Python) recovers EIP-191 `personal_sign` and hand-hashed EIP-712 signatures produced by `cast wallet sign` / `cast wallet sign --data --from-file` | verified in a scratch venv |
| ERC-1271 selector `0x1626ba7e`, magic return; EIP-7702 designator prefix `0xef0100` | assumed from the EIPs (not exercised) |
| Browsers send SameSite=Strict cookies on same-site `fetch`/`EventSource`/`WebSocket`; refuse `ACAO: *` with credentials | assumed (standard behavior; browser smoke run is the proof) |
| cloudflared passes `Set-Cookie`, `Authorization`, and WebSocket upgrades; path allowlist is the only filter | assumed (must add paths to the regex) |
| Permit2 canonical address has code on 4663 (`DOMAIN_SEPARATOR()` answered) | verified; not used by units 0-2 |

## Tradeoffs accepted

- We accept one long-lived secret in an HttpOnly cookie (7 d) instead of a short session + refresh in exchange for one credential type and one code path; mitigation is `logout` = revoke and the owner/CLI `keys revoke`.
- We accept that a stolen bearer grants the wallet's full entitlement (no scopes) in exchange for wallet-level semantics that stay true when the owner changes thresholds.
- We accept a 30 s oracle cache plus grace, so a fresh holder can wait up to 30 s to be recognized, in exchange for at most one `balanceOf` per wallet per 30 s regardless of key or stream count.
- We accept a new pure-Python dependency (`eth-keys`) rather than hand-rolling secp256k1 recovery or calling the `ecrecover` precompile through the RPC (which would make sign-in depend on the node).
- We accept a small RFC 6455 implementation in-tree (server-push only) rather than a dependency the stdlib server cannot host.
- We accept that gate.sqlite is a second SQLite file written by two processes (server, CLI); writes are rare and WAL + busy_timeout cover it.

## Open questions and risks

- Should `flags` default to threshold 0 (any signed-in wallet) at launch so the tags are visible before the token trades, or stay unset until the policy is signed?
- Is a 7-day browser session acceptable, or does the owner want the cookie to expire with the browser (`Max-Age` omitted) at the cost of daily sign-ins?
- Do programmatic keys need a wallet-visible "revoke all" from the terminal, given a leaked key is only stoppable by the wallet holder or the owner?
- Risk: relying on `latest` balance means an unfinalized reorg could grant/deny for one 30 s window; grace absorbs the deny side, and the grant side is bounded by cache TTL. Is that acceptable, or should the oracle pin `finalized`?
- Risk: the tunnel allowlist is the only thing keeping `/api/gate/*` unreachable until the deploy edit lands; the golden test does not cover cloudflared.

## Next implementation step

Write `gate.py` types plus `GatePolicy.digest()`/`typed_data()` and the SIWE verifier, with the checked-in `cast wallet sign` fixture signatures as the first tests.
