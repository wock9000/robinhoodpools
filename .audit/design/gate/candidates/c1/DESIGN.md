# Gate design, candidate 1: stateful sessions and hashed keys

Scope: TOKEN_PLAN units 0–2. Policy, holding oracle, SIWE sign-in, API keys,
gated REST/SSE/WebSocket, per-key limits, cookie/CSRF rules, lp_server.py
integration, header UI, test harness. Direction: server-side session rows and
opaque hashed API keys in a dedicated gate SQLite; entitlement computed per
request from a cached `Holding`.

## Problem

rhpools is a stdlib `ThreadingHTTPServer` with no identity of any kind: every
JSON route is anonymous, `ACAO: *`, memoized by publication key and ETag, and
the tunnel only forwards an exact path allowlist. We need to add four
holder-gated features (trade, lp, api, flags) without changing a byte of any
anonymous response, without a second writer on the 652 GB market SQLite,
without the server ever touching a private key, and before the token exists.
The awkward parts: (1) a holder's entitlement is a fact about the chain that
changes under them, so it must be re-derived from a cached balance rather than
baked into a credential; (2) the same entitlement must reach three transports
(REST, SSE, a new WebSocket) and a browser header; (3) policy changes must be
provable to have come from one pinned owner address, yet a CLI on the host
must still work when the web path is down; (4) existing memoization and public
caching would leak a holder's tagged payload to anonymous users unless the gate
participates in the cache key and cache headers.

## Usage (caller's view)

### Programmatic client (README excerpt)

```
# 1. Sign in once from the browser, mint a key under WALLET › KEYS. Shown once.
export RHP_KEY=rhp_k1_7Qp3nL9x.…

# 2. REST: same routes as anonymous; a key raises your quota and, if your wallet
#    holds ≥ flags threshold, adds tag fields to tape/activity rows.
curl -H "Authorization: Bearer $RHP_KEY" https://rhpools.lol/api/v1/pools?limit=50
#    → 200, X-RateLimit-Limit: 600, X-RateLimit-Remaining: 599, Cache-Control: private, no-store
#    → 403 {"error":"holding below api threshold","gate":{"feature":"api","balance":"…","minimum":"…"}}
#    → 429 {"error":"key quota exhausted"} Retry-After: 7

# 3. Stream: SSE or WebSocket on the same URL. Closes with 4403 / event: gate
#    when your balance stays below threshold past the grace window.
curl -N -H "Authorization: Bearer $RHP_KEY" https://rhpools.lol/api/v1/stream?channel=activity
websocat -H "Authorization: Bearer $RHP_KEY" wss://rhpools.lol/api/v1/stream?channel=activity

# 4. Anonymous calls are untouched: no key, no header, same bytes as today.
```

### Call site 1: a gated resource inside the server (units 3–5 use this)

```python
# lp_server.py, inside _bounded(); the only new lines on the hot path
grant = self.gate.grant(self.headers, client_ip=self._client_ip())
payload = method(query, grant) if route.gated else method(query)
key = _publication_key(path, query, payload)
if key is not None and grant.features:
    key = (*key, ("gate", *sorted(grant.features)))
self._json(200, payload, key=key, fresh=freshness(route, query), private=bool(grant.features))

# workbench-style trade route (unit 4) — one call, fail closed
grant = self.gate.require(grant, "trade")          # raises GateDenied(401|403)
result = self.runtime.trade.quote(payload, grant.principal.wallet)
```

### Call site 2: the terminal header (lp_gate.js)

```js
// lp_terminal.js: one line; lp_gate.js owns the state machine and the DOM.
Gate.mount(document.getElementById("gate-strip"), { onChange: (status) => reconnectStream() });

// lp_gate.js internals, caller-visible shape
const status = await Gate.status();     // GET /api/gate/status  → GateStatus
await Gate.signIn();                    // nonce → personal_sign → POST /api/gate/session (cookie set)
const key = await Gate.mintKey("bot");  // POST /api/gate/keys → { key: "rhp_k1_…", prefix, label }
// GateStatus = { wallet, features, holding: {balance, block, observed_at}, grace: {api: until},
//                policy: {token, version, minimum, grace_s}, owner: false, rate: {...} }
```

### Call site 3: owner changes thresholds

```
# Web: owner signs in (same SIWE), header shows OWNER, opens POLICY, submits
# a draft; server returns canonical typed data; wallet eth_signTypedData_v4.
POST /api/gate/policy/draft {"token":"0x…","decimals":18,"trade_min":"1000","lp_min":"1000","api_min":"5000","flags_min":"100","grace_s":900}
  → {"typed_data": {...GatePolicy v(n+1)...}, "digest": "0x…"}
POST /api/gate/policy {"message": {...}, "signature": "0x…"}   → 200 {policy}  |  403 signer is not owner

# Host fallback (unit down, wallet lost): audited as actor "host-cli:andnasnd"
rhpools-gate --gate-db ~/.local/share/rhpools/gate.sqlite policy set \
    --token 0x… --decimals 18 --trade-min 1000 --lp-min 1000 --api-min 5000 --flags-min 100 --grace-s 900
rhpools-gate policy show | keys list --wallet 0x… | keys revoke --prefix 7Qp3nL9x | audit tail
```

## Shape

### Data structures

```python
Feature = Literal["trade", "lp", "api", "flags"];  FEATURES = ("trade", "lp", "api", "flags")

@dataclass(frozen=True)
class GatePolicy:
    """Owner-signed thresholds. Single source of truth for 'who is a holder'."""
    version: int                     # strictly increasing; row PK in gate DB
    token: str | None                # lowercase address; None = not launched
    decimals: int
    minimum: Mapping[Feature, int]   # raw units; {} when token is None
    grace_s: int
    issued_at: int                   # unix seconds; replay window ±600 s
    signer: str                      # owner address (web) or "host-cli:<user>"

@dataclass(frozen=True)
class Holding:
    wallet: str; balance_raw: int | None      # None = oracle could not observe
    block: int; observed_at: float; policy_version: int
    qualified_at: Mapping[Feature, float]     # last observed_at with balance ≥ minimum[f]

@dataclass(frozen=True)
class Entitlement:
    wallet: str; features: frozenset[Feature]; holding: Holding
    grace_until: Mapping[Feature, float]      # only features held by grace

class Principal(NamedTuple):
    wallet: str; via: Literal["session", "key"]; ref: bytes   # sha256 of cookie/key
    is_owner: bool

@dataclass(frozen=True)
class Grant:
    """What one request is allowed to do. ANONYMOUS has no principal."""
    principal: Principal | None; entitlement: Entitlement | None
    features: frozenset[Feature]               # derived; () for anonymous
    def has(self, feature: Feature) -> bool: ...
    @property
    def keyed(self) -> bool: ...               # via == "key"
```

Dominant access patterns traced through these:

- Every request: `headers → Grant`. Cookie/bearer → sha256 → in-memory
  session/key cache (dict, 4096 entries, 60 s TTL, backed by gate DB) → wallet
  → oracle cache (dict per wallet, 30 s TTL) → `entitle(policy, holding, now)`.
  Two dict reads and one pure function; no SQLite on the hot path once warm.
- Stream every 30 s: `gate.refresh(grant)` re-runs the same path; the stream
  closes when `"api" ∉ grant.features`.
- Holder sells: next oracle refresh sees `balance < minimum`; `qualified_at`
  keeps the feature alive until `qualified_at + grace_s`, then it drops.
- Policy change: new `version` row; oracle cache cleared; `qualified_at` on
  stored holdings ignored when `holding.policy_version != policy.version`.
- Restart: sessions, keys, holdings (with `qualified_at`) survive in gate DB;
  nonces and rate buckets do not (harmless: a pending sign-in retries).

### Gate DB (own SQLite file, `<data-dir>/gate.sqlite`, WAL, one connection behind a lock)

```
policy   (version INTEGER PK, token TEXT NULL, decimals INT, minimum_json TEXT, grace_s INT,
          issued_at INT, signer TEXT, signature TEXT NULL, applied_at REAL)
audit    (id INTEGER PK, at REAL, actor TEXT, kind TEXT, outcome TEXT, detail_json TEXT)
session  (id_hash BLOB PK, wallet TEXT, created REAL, expires REAL, last_seen REAL, revoked_at REAL NULL)
api_key  (key_hash BLOB PK, prefix TEXT UNIQUE, wallet TEXT, label TEXT, created REAL,
          last_used REAL NULL, revoked_at REAL NULL)
holding  (wallet TEXT PK, balance_raw TEXT NULL, block INT, observed_at REAL,
          policy_version INT, qualified_json TEXT)
```

uint256 values are decimal TEXT (SQLite ints are 64-bit). Secrets are never
stored: `id_hash`/`key_hash` are sha256 of the 32 random bytes the client
holds. Writes are rare (sign-in, mint, one holding upsert per wallet per 30 s,
policy); the market writer is never touched.

### Data flow through signatures

```python
# src/rhpools/gate/service.py — the one public surface. Transport-free.
class Gate:
    def __init__(self, config: GateConfig, store: GateStore, oracle: HoldingOracle, clock=time.time): ...
    @classmethod
    def open(cls, config: GateConfig) -> "Gate":  raise NotImplementedError   # opens DB, loads policy head
    def close(self) -> None:                        raise NotImplementedError

    # --- per request (hot path) ---
    def grant(self, headers: Message, *, client_ip: str) -> Grant:
        """Bearer key wins over cookie. A malformed, unknown or revoked bearer raises
        GateDenied(401, "invalid api key"): a programmatic caller asked for the keyed tier and
        must hear that it failed. An unknown or expired cookie yields ANONYMOUS: the browser's
        natural fallback is the anonymous page, and the UI re-reads /api/gate/status."""
        raise NotImplementedError
    def require(self, grant: Grant, feature: Feature) -> Grant:
        """401 no principal; 403 with balance/minimum detail when below threshold;
        403 'gate not configured' when policy.token is None. Fail closed."""
        raise NotImplementedError
    def refresh(self, grant: Grant) -> Grant:      raise NotImplementedError   # streams, every 30 s
    def take(self, grant: Grant, cost: int = 1) -> RateVerdict:
        """Per-key token bucket (config.key_rpm, burst). Anonymous → no-op verdict.
        Raises GateDenied(429, retry_after)."""
        raise NotImplementedError

    # --- identity ---
    def nonce(self) -> SiweChallenge:               raise NotImplementedError   # 16 bytes, 5 min, memory
    def sign_in(self, message: str, signature: str, *, host: str, client_ip: str) -> tuple[Grant, SessionCookie]:
        """Parse EIP-4361; domain ∈ config.hosts; chain 4663; nonce consumed;
        signer == message.address (ECDSA, else EIP-1271 eth_call when address has code)."""
        raise NotImplementedError
    def sign_out(self, grant: Grant) -> None:       raise NotImplementedError
    def mint_key(self, grant: Grant, label: str) -> MintedKey:   # requires session + "api"; ≤ 5 live keys
        raise NotImplementedError
    def revoke_key(self, grant: Grant, prefix: str) -> None:     raise NotImplementedError
    def list_keys(self, grant: Grant) -> list[ApiKeyView]:       raise NotImplementedError

    # --- policy ---
    @property
    def policy(self) -> GatePolicy:                 raise NotImplementedError   # reloaded when DB head moves
    def policy_draft(self, grant: Grant, fields: Mapping[str, object]) -> TypedData:
        """Owner only. version = head+1, issued_at = now. Returns canonical EIP-712 JSON."""
        raise NotImplementedError
    def apply_signed_policy(self, message: Mapping[str, object], signature: str) -> GatePolicy:
        """Rebuilds typed data from `message` (client `types` ignored), recovers signer,
        requires signer == config.owner, version == head+1, |now-issued_at| ≤ 600.
        Audit row on success AND on refusal."""
        raise NotImplementedError
    def apply_host_policy(self, policy: GatePolicy, actor: str) -> GatePolicy:  raise NotImplementedError
    def status(self, grant: Grant) -> GateStatus:  raise NotImplementedError

@dataclass(frozen=True)
class GateConfig:
    db_path: Path; owner: str | None; hosts: frozenset[str]     # hosts derived from --public-origin
    rpc_url: str = "http://127.0.0.1:8547"; chain_id: int = 4663
    session_ttl_s: int = 7 * 86400; key_rpm: int = 600; key_burst: int = 60
    keyed_slots: int = 8; keyed_streams: int = 64; streams_per_key: int = 4
    signin_per_ip_per_min: int = 5; cookie_name: str = "__Host-rhp_session"

class GateDenied(Exception):
    status: int; reason: str; retry_after: int | None; detail: Mapping[str, object]
```

```python
# src/rhpools/gate/oracle.py
class HoldingOracle:
    def __init__(self, rpc: Callable[[list[tuple[str, list]]], list], store: GateStore, ttl_s: int = 30, max_wallets: int = 8192): ...
    def holding(self, wallet: str, policy: GatePolicy) -> Holding:
        """Cached 30 s; single-flight per wallet; batch [eth_blockNumber, eth_call balanceOf] to config.rpc_url.
        On RPC failure returns the last Holding (balance kept) if observed_at within policy.grace_s,
        else Holding(balance_raw=None). Persists changed rows to gate DB."""
        raise NotImplementedError
    def forget_all(self) -> None:  raise NotImplementedError   # on policy change

def entitle(policy: GatePolicy, holding: Holding, now: float) -> Entitlement:
    """Pure. feature ∈ features iff balance ≥ minimum[f], or now ≤ qualified_at[f] + grace_s
    with holding.policy_version == policy.version. Token unset → empty."""
    raise NotImplementedError
```

```python
# src/rhpools/gate/policy.py  (pure; eth-abi + eth-keys)
EIP712_DOMAIN = {"name": "rhpools gate", "version": "1", "chainId": 4663}      # no verifyingContract
GATE_POLICY_TYPE = "GatePolicy(uint64 version,address token,uint8 decimals,uint256 tradeMin,uint256 lpMin,uint256 apiMin,uint256 flagsMin,uint32 graceSeconds,uint64 issuedAt)"
def policy_typed_data(policy: GatePolicy) -> TypedData:    raise NotImplementedError  # uints as decimal strings for wallets
def policy_digest(policy: GatePolicy) -> bytes:            raise NotImplementedError  # keccak(0x1901‖domain‖struct)
def parse_policy_message(message: Mapping) -> GatePolicy:  raise NotImplementedError  # ValueError on any bad field
def recover(digest: bytes, signature: str) -> str:         raise NotImplementedError  # eth-keys; v ∈ {0,1,27,28}

# src/rhpools/gate/siwe.py  (pure except the 1271 hook)
@dataclass(frozen=True)
class SiweMessage: domain: str; address: str; statement: str; uri: str; version: str; chain_id: int; nonce: str; issued_at: str; expiration_time: str | None
def build(domain, address, nonce, issued_at, expires, statement) -> str: raise NotImplementedError
def parse(text: str) -> SiweMessage:                                     raise NotImplementedError  # strict EIP-4361 grammar
def personal_digest(text: str) -> bytes:                                 raise NotImplementedError  # EIP-191 0x45
def verify(message: SiweMessage, signature: str, *, is_contract: Callable[[str], bool], eip1271: Callable[[str, bytes, bytes], bool]) -> None:
    raise NotImplementedError   # raises SiweError
```

```python
# src/rhpools/gate/store.py — sqlite3, one connection, lock, WAL, busy_timeout 2000
class GateStore:
    def __init__(self, path: Path): ...
    def policy_head(self) -> GatePolicy | None; def insert_policy(self, p: GatePolicy, signature: str | None) -> None  # IntegrityError = version race
    def data_version(self) -> int                                                     # PRAGMA data_version, cheap head check
    def audit(self, actor: str, kind: str, outcome: str, detail: Mapping) -> None
    def session(self, id_hash: bytes) -> SessionRow | None; def open_session(...); def close_session(...); def touch_session(...)
    def key(self, key_hash: bytes) -> ApiKeyRow | None; def insert_key(...); def revoke_key(...); def keys_for(wallet) -> list
    def holding(self, wallet) -> Holding | None; def upsert_holding(self, h: Holding) -> None
    def purge(self, now: float) -> None            # expired sessions, hourly

# src/rhpools/gate/http.py — HTTP framing for gate routes; the only module that sees the handler
class GateHttp:
    def __init__(self, gate: Gate): ...
    def get(self, handler, path: str, query: dict) -> bool:     raise NotImplementedError  # /api/gate/{status,nonce,keys,policy}, /api/v1/stream
    def post(self, handler, path: str, payload: dict) -> bool:  raise NotImplementedError  # session, session/logout, keys, keys/revoke, policy/draft, policy
    def keyed_stream(self, handler, grant: Grant, query: dict) -> None:  raise NotImplementedError  # SSE or RFC 6455 upgrade

# src/rhpools/gate/stream.py — transports for the keyed stream
class EventSink(Protocol):
    def event(self, id: str, name: str, body: bytes) -> None; def heartbeat(self) -> None
    def close(self, code: int, reason: str) -> None
class SseSink(EventSink): ...                      # id:/event:/data: framing, identical bytes to /api/lp/stream
class WebSocketSink(EventSink): ...               # websockets.server.ServerProtocol (sans-I/O) over handler.request

# src/rhpools/gate/cli.py — console script `rhpools-gate`; opens GateStore directly (no HTTP)
def main(argv: list[str] | None = None) -> int:  raise NotImplementedError
```

### Integration in lp_server.py (all additions; anonymous paths unchanged)

```python
class Route(NamedTuple):           # + gated: bool = False   (gated resources take (query, grant))
Runtime.__init__:  self.gate = Gate.open(GateConfig.from_args(args)); self._resources.callback(self.gate.close)
                   self.gate_http = GateHttp(self.gate)
parser():  --gate-db (default <data-dir>/gate.sqlite), --gate-owner (RHP_GATE_OWNER), --gate-rpc-url (RHP_GATE_RPC_URL, default http://127.0.0.1:8547)
_ASSETS:   + lp_gate.js, lp_gate.css                      # tunnel regex and CSP already cover /static/* patterns once added
_json():   + private: bool  → "Cache-Control: private, no-store" and "Vary: Accept-Encoding, Cookie, Authorization"; ACAO stays "*"
_bounded(): grant = gate.grant(...); if grant.keyed: grant = gate.require(grant, "api") (403 below api threshold, fail closed);
            keyed requests use gate.keyed_slots instead of the lane semaphore; gate.take(grant); method(query, grant) when route.gated;
            key += ("gate", *features) when features; private when features; X-RateLimit-* headers when keyed
_lp_stream(): body extracted to _pump_lp_stream(query, grant, sink); anonymous call passes SseSink(self) → same bytes.
            Every 30 s: grant = gate.refresh(grant); flags gating for tags flips at the next event.
do_GET():   if path.startswith("/api/gate/") or path == "/api/v1/stream": return self.runtime.gate_http.get(self, path, query)
do_POST():  gate paths added to the allowlist; _same_origin() + JSON content type already enforced; then gate_http.post(...)
do_OPTIONS(): unchanged for existing routes; /api/gate/* and /api/v1/stream answer 204 with Allow-Headers "Accept, Content-Type, Authorization"
_client_ip(): CF-Connecting-IP when Host is a public origin, else peer address
```

New tunnel paths: `/api/gate/(status|nonce|session|session/logout|keys|keys/revoke|policy|policy/draft)`,
`/api/v1/stream`, `/static/lp_gate\.(css|js)`. The systemd unit gains
`Environment=RHP_GATE_OWNER=0x…` and `Environment=RHP_GATE_RPC_URL=http://127.0.0.1:8547`.

### HTTP rules

- Cookie: `__Host-rhp_session=<base64url 32 B>; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=604800`.
  No `Domain`. Loopback dev works because browsers treat `localhost` as a secure context.
- CSRF: every gate POST needs `_same_origin()` (Origin header) AND `Content-Type: application/json`
  (non-simple → preflight, and OPTIONS never allows POST) AND the SameSite=Strict cookie. No token needed.
- CORS: `ACAO: *` unchanged everywhere. Cookies never ride cross-site (Strict). Bearer keys are usable
  from non-browser clients and same-origin pages; cross-origin browser use of keys is not a goal and the
  existing OPTIONS bytes stay identical.
- Caching: any response whose body depends on the grant is `private, no-store` with `Vary: Cookie, Authorization`
  and a memo key that includes the feature set, so ETags never collide between anonymous and holder variants.
- Auth endpoints: `POST /api/gate/session` is limited to 5/min per client IP and 60/min per process
  (ECDSA recovery is ~9 ms in pure Python). Refusals are audited with kind `signin_rejected`.
- Errors: 401 `{"error":"sign in required"}`; 403 `{"error":"holding below <f> threshold","gate":{...}}` or
  `{"error":"gate not configured"}`; 429 `{"error":"key quota exhausted"}` + `Retry-After`. Streams send
  `event: gate` / WS close code 4403 with the same reason, after a final `{"state":"grace","until":…}` while in grace.

### Header UI (lp_gate.js + lp_gate.css, vanilla, CSP-clean)

States rendered in `#gate-strip` (a `.strip-state` sibling after the clock) and a `<kbd>w</kbd>wallet`
key-control opening `<dialog id="gate-dialog">` (KEYS list/mint/revoke; POLICY panel when `owner`):

```
NO WALLET          window.ethereum absent → strip says "WALLET —", dialog explains read-only mode
CONNECT            wallet present, no account → button "connect"
WRONG CHAIN        account on chain ≠ 4663 → "switch to 4663" (wallet_switchEthereumChain, as workbench.js)
SIGN IN            connected, no session → "sign in" → personal_sign → cookie
HOLDER  [ok] trade lp api flags     features non-empty → green; per-feature dots
GRACE   mm:ss      any feature held by grace → yellow countdown from grace_until
NOT HOLDER         session, token set, balance < every minimum → "hold ≥ X TOKEN"; balance shown
TOKEN NOT LAUNCHED policy.token null → dim; sign-in still offered (so keys can be prepared)
OWNER              principal.is_owner → suffix "· OWNER", POLICY tab enabled
```

`Gate.status()` polls `/api/gate/status` every 30 s while a session exists, and on `accountsChanged` it signs
out (server session is per wallet; a different account means a new SIWE). Sign-out clears the cookie
server-side (`Max-Age=0`). Status polling stops when the tab is hidden.

### Invariants (where each is enforced)

1. Server never sees a private key: only `personal_sign`/`eth_signTypedData_v4` results cross the wire (types: `signature: str`, verified by recovery).
2. Anonymous responses are byte-identical: `grant.features == ()` ⇒ `private=False`, memo key unchanged, no new headers. Enforced by the golden test.
3. Gate never touches the market writer: `GateStore` owns `gate.sqlite`; nothing in `gate/` imports the market store.
4. Fail closed: `require()` raises unless `feature ∈ grant.features`; oracle failure yields `balance_raw=None` after grace → no features.
5. One policy source: `policy.version` PK; `apply_*` inserts head+1 or fails; the CLI and web race resolve by the DB, not by memory.
6. Owner-only changes: `apply_signed_policy` compares the recovered address to `config.owner` (pinned in the unit); the CLI path requires host shell access and is audited with the OS user.
7. Secrets shown once: `MintedKey.key` is returned from `mint_key` and never stored; DB holds sha256 + 8-char prefix.
8. Idempotence: sign-in twice → two sessions (each valid, both listed under KEYS as "sessions"); mint twice → two keys; policy apply twice with the same version → second refused as `version_conflict` (audited); crash after `insert_policy` but before cache reload → next `policy` property read sees the new head via `data_version`.

### What the surface hides

Callers see `Grant`, `require`, `take`, `refresh`, and five identity calls. Hidden: credential parsing, hashing, two caches with TTLs and single-flight, EIP-191/EIP-712/EIP-1271, SIWE grammar, grace bookkeeping, policy versioning and audit, rate buckets, SSE/WS framing. Nothing transport-shaped leaks into `Gate`; `GateHttp` is the only module that knows about `BaseHTTPRequestHandler`, and `lp_server.py` gains about 40 lines.

## Synthesis decision

Left for arena.

## Tradeoffs accepted

- We accept a second SQLite file and a per-thread-locked connection in exchange for sessions, keys, grace state and the audit log surviving restarts and never touching the market writer.
- We accept a 30 s oracle lag plus a grace window in exchange for no per-request RPC and no revocation flapping during a holder's own sell.
- We accept pure-Python ECDSA (~9 ms, measured) with an optional `coincurve` extra in exchange for adding only `eth-keys` (deps: eth-typing, eth-utils, both already locked) to `pyproject.toml`.
- We accept that an existing anonymous route with a session cookie and `flags` returns `private, no-store` (no ETag/304) in exchange for never leaking a tagged payload through shared caches.
- We accept hand-driving `websockets.server.ServerProtocol` (sans-I/O) inside a handler thread in exchange for WebSocket support with zero new dependencies and no asyncio in the process.
- We accept that keys are not usable cross-origin from browsers (OPTIONS unchanged) in exchange for byte-identical preflight responses.
- We accept per-process rate buckets (reset on restart) because the service is a single process behind one tunnel.

## Alternatives considered

Full text in RATIONALE.md. In brief: stateless signed cookies/JWT lost because entitlement must be re-derived per request anyway and revocation needs state; per-request `balanceOf` lost on RPC load and flapping; putting gate tables in the market DB lost on the writer contention constraint; a separate auth process (proxy) lost on the tunnel allowlist and the CSP/CSRF surface it would add; `eth-account` lost on dependency weight; asyncio `websockets.serve` lost because it would mean a second server and port.

## Open questions and risks

- Should the owner address be exempt from thresholds (useful before launch, but a standing backdoor), or should a host-config `--gate-preview-wallets` list exist for pre-launch testing?
- Is `__Host-` acceptable given some older WebViews reject Secure cookies on `http://localhost` during development, or should loopback mode fall back to a plain `rhp_session` name?
- Do we want sessions bound to the wallet only, or also to a client fingerprint (User-Agent hash) to limit cookie theft blast radius at the cost of breaking on browser updates?
- Is 5 live keys per wallet and 4 concurrent streams per key the right ceiling for launch?
- EIP-1271 verification for smart-account holders adds an `eth_call` on sign-in only; is supporting contract wallets in scope for launch?
- The local Nitro at 8547 is the only oracle source by default; if it lags, holders see stale balances for up to `grace_s`. Should the oracle cross-check `eth_blockNumber` against `/api/lp/status` head and refuse to grant on a lagging node?

## Next implementation step

Write `gate/policy.py` and `gate/siwe.py` as pure functions with the EIP-712 and EIP-191 vectors from the probe below as their first tests, then `GateStore` with the schema above.

## Test and verification harness

```
tests/test_gate_policy.py      EIP-712 digest vector (below); owner accepted; non-owner refused + audited; version must be head+1; issued_at window
tests/test_gate_siwe.py        build/parse round trip; tampered nonce/domain/chain/expiry; bad v; EIP-1271 hook stubbed
tests/test_gate_oracle.py      entitle(): threshold edge (== minimum grants), grace expiry, policy_version mismatch, balance None after grace; FakeRPC single-flight
tests/test_gate_server.py      serving() fixture + Gate on tmp gate.sqlite with FakeRPC: sign-in sets cookie flags; CSRF (missing Origin, wrong content type) 403;
                               mint key shown once; bearer on /api/v1/pools → X-RateLimit + private; 429 after burst; revoked key → 401;
                               key whose wallet is below api threshold → 403 with balance/minimum detail;
                               /api/v1/stream SSE closes with event: gate after balance drop past grace (clock injected); WS variant closes 4403
tests/test_lp_server_golden.py tests/golden/anonymous_routes.json is recorded once at 1b00978 by tests/record_golden.py (deterministic test service,
                               every existing GET/HEAD/OPTIONS route incl. first 3 SSE frames, `as_of` normalized); the test replays it against the
                               gated server with no credentials and asserts status, headers and body bytes are equal
tests/test_gate_fork.py        anvil fork (skips if ~/.foundry/bin/anvil missing or RHP_TEST_FORK_URL unset): deploy tests/fixtures/standin_erc20.hex from
                               account 0, mint to account 1, real oracle against the fork, sign SIWE with account 1's key via eth-keys, mint API key,
                               transfer below threshold, advance injected clock past grace, assert 403 and stream close
tests/test_gate_cli.py         rhpools-gate policy set writes head+1 with actor host-cli; running Gate observes it via data_version
```

Browser proof (unit 1): open `/` on loopback, connect anvil account 1 in the wallet, sign in, header reads
`HOLDER [ok] trade lp api flags`; DevTools shows the cookie with HttpOnly/Secure/SameSite=Strict; sell in the
wallet, header goes `GRACE 14:59` then `NOT HOLDER`. curl proof (unit 2) is the README block above.

## Chain and tooling facts relied on

| Fact | Status | How |
|---|---|---|
| Local Nitro RPC 127.0.0.1:8547 answers chain id 4663, head 74143233, batch JSON-RPC works | verified | `cast chain-id`, curl batch `[eth_blockNumber, eth_call]` |
| `balanceOf(address)` = `0x70a08231`, `decimals()` = `0x313ce567`, `isValidSignature(bytes32,bytes)` = `0x1626ba7e` | verified | `cast sig` |
| anvil 1.7.1 forks 8547 with `--chain-id 4663`; `forge create` of a 4290-byte stand-in ERC-20, `mint`, `transfer`, `balanceOf`, `evm_increaseTime` all work | verified | run on port 8599, killed afterwards |
| SIWE (EIP-4361 text, EIP-191 prefix) signed by `cast wallet sign` recovers to the signer with eth-keys `NativeECCBackend` in ~9 ms | verified | `/tmp/gate-erc20/verify_sigs.py` |
| Hand-rolled EIP-712 (domain name/version/chainId, no verifyingContract; struct above) matches `cast wallet sign --data`; digest `83f209e5…626dd` for the vector in the probe; non-owner key recovers to a different address | verified | same probe |
| `eth-keys` 0.8.0 depends only on eth-typing ≥3 and eth-utils ≥2 (both already in uv.lock); `coincurve` optional | verified | PyPI metadata |
| pycryptodome (already a dep) has no secp256k1, so a new dependency is unavoidable | verified | `ECC.generate(curve="secp256k1")` fails |
| `websockets` 15.0.1 (already a dep) exposes sans-I/O `ServerProtocol`; upgrade + text frames + close 4403 work over `BaseHTTPRequestHandler` | verified | `/tmp/gate-erc20/ws_probe.py` |
| USDG `decimals()` returns 6 on chain (scout inferred 18) | verified | `cast call` |
| Wallets (MetaMask, cast) require uint256 EIP-712 values as decimal strings in JSON | verified for cast | cast rejected numeric JSON; MetaMask [ASSUMED] |
| Browsers treat `http://localhost` as a secure context so `__Host-`/Secure cookies work in loopback dev | assumed | not exercised in this run |
| cloudflared sets `CF-Connecting-IP` on tunneled requests | assumed | standard header, not observed here |
| Permit2 `0x0000…78BA3` has code on 4663 | verified | `eth_getCode` (not used by this design) |
