# Gate candidate 2: stateless capabilities

Units 0-2 of TOKEN_PLAN.md. Direction: after SIWE the server issues short-lived
HMAC-signed capability tokens; no session table; API keys and browser sign-ins
are both long-lived *grants* that exchange for capabilities; revocation is by
policy version (global) and wallet key epoch (per wallet).

## Problem

rhpools is a stdlib `ThreadingHTTPServer` behind a cloudflared path allowlist,
serving anonymous, `ACAO: *`, publicly cacheable JSON and SSE, with a 652 GB
market SQLite whose writer is already contended. We must add holder-gated
features (trade, lp, api, flags) without changing one byte of any anonymous
response, without a session store that the market writer or a second daemon has
to own, and without the server ever holding a key. The wallet is the identity
(EIP-4361 sign-in), the entitlement is a chain read (`balanceOf` ≥ owner-set
threshold), and the only mutable truth the gate needs is the owner's policy.
Everything else (who is signed in, which key exists, who is in grace) can be a
signed claim the client carries back, so the hot path is an HMAC verify with
zero I/O and the persistent gate state is one append-only audit table.

Constraints honored: `_ROUTES`/`_bounded`/`_json`/`_sse` shapes stay; POST
gating reuses `_same_origin`; the token may be unset (every gated feature
refused, sign-in still works); a stand-in ERC-20 on an anvil fork must drive the
whole proof; no new JS dependency (raw EIP-1193 `personal_sign` and
`eth_signTypedData_v4`, like `workbench.js`).

## Usage (caller's view)

### Programmatic client (README excerpt)

```sh
# 1. In the terminal: connect wallet → SIGN IN → KEYS → MINT. Copy the key once.
export RHP_KEY=rhk1_...

# 2. Exchange the key for a 5-minute capability. Keep the last one and send it
#    back as `prior`; that is what carries your grace window across a sale.
CAP=$(curl -s -X POST https://rhpools.lol/api/gate/token \
  -H "Authorization: Bearer $RHP_KEY" -H 'Content-Type: application/json' \
  -d "{\"prior\": ${PRIOR_CAP:-null}}" | jq -r .capability)

# 3. Use it. Same routes as the public API; holders get gated fields and the
#    keyed lane instead of the anonymous capacity lane.
curl -s -H "Authorization: Bearer $CAP" 'https://rhpools.lol/api/lp/tape?window=1h'
curl -sN -H "Authorization: Bearer $CAP" 'https://rhpools.lol/api/lp/stream?channel=activity'
# The stream ends with `event: gate` `{"reason":"expired"}` at capability expiry;
# exchange again (with prior) and reconnect with Last-Event-ID.
```

Refusals are JSON with a machine-readable reason:
`403 {"error":"...","gate":{"feature":"api","reason":"below_threshold","balance":"…","threshold":"…"}}`,
`401 {"gate":{"reason":"expired"|"policy_changed"|"invalid"|"not_signed_in"}}`,
`429` with `Retry-After` for per-key rate limits.

### Browser (terminal header)

```js
// static/lp_gate.js — cookies are HttpOnly; JS only ever sees claims.
const nonce = await gateFetch("GET", "/api/gate/nonce");
const message = siweMessage({ address, nonce: nonce.nonce, issuedAt: nonce.issued_at });
const signature = await window.ethereum.request({ method: "personal_sign", params: [hexUtf8(message), address] });
const me = await gateFetch("POST", "/api/gate/siwe", { message, signature }); // sets rhp_grant + rhp_cap
renderGateStrip(me.claims);          // {wallet, features:["flags"], expires, since_ok, holding:{balance,threshold}}
scheduleRefresh(me.claims.expires);  // POST /api/gate/token 30 s before expiry; cookie grant, cookie prior
```

### Owner policy change (same file, owner-only panel)

```js
const typed = policyTypedData({ token, decimals, tradeMin, lpMin, apiMin, flagsMin, graceSeconds, version: status.policy_version + 1, deadline });
const signature = await window.ethereum.request({ method: "eth_signTypedData_v4", params: [address, JSON.stringify(typed)] });
await gateFetch("POST", "/api/gate/policy", { policy: typed.message, signature });
```

### Host CLI fallback

```sh
rhpools-gate show
rhpools-gate set-policy --token 0x… --decimals 18 --min trade=1000 --min flags=10 --grace 900   # host actor, audit row source=cli
rhpools-gate revoke-wallet 0x…    # bumps key epoch: every grant/key of that wallet dies at next exchange
rhpools-gate rotate-secret        # new HMAC secret file: every outstanding token dies at once
```

### Server-side call sites (`lp_server.py`)

```python
# Route table: gated adds a feature name; anonymous behavior of the route is untouched.
"/api/lp/tape": Route("lp", "tape", "fast", (2, 10), gated="flags"),

# _bounded: one call decides everything about identity for this request.
cap = self._capability()                       # Capability | None; None ⇒ anonymous path, byte-identical
if route.gated and cap is not None and route.gated in cap.features:
    payload = self.runtime.flags.decorate(payload, cap)   # unit 3 owns the field shapes
    self._json(200, payload, key=(*key, cap.features), fresh=freshness(route, query), private=True)

# Unit 4/5 POST routes:
cap = self._require("trade")                   # raises GateRefusal → 401/403 JSON with reason
result = self.runtime.trade.quote(payload, wallet=cap.wallet)
```

## Shape

### Data structures

```python
# gate.py ---------------------------------------------------------------------
class Feature(enum.IntFlag):
    """Bit positions are the wire encoding inside capabilities; never renumber."""
    TRADE = 1; LP = 2; API = 4; FLAGS = 8

@dataclass(frozen=True, slots=True)
class Capability:
    """Short-lived proof of entitlement. Self-contained; valid iff mac ok, now < exp,
    policy_version == current. Carries the grace memory (since_ok)."""
    wallet: bytes            # 20 bytes
    features: Feature
    policy_version: int
    epoch: int               # wallet key epoch at issue; checked at issue, not per request
    subject: bytes           # 8 bytes: key_id for api grants, zeros for browser; rate-limit key
    issued_at: int
    expires_at: int          # issued_at + CAP_TTL_S (300)
    since_ok: int            # last unix second holding was observed ≥ threshold; 0 = never

@dataclass(frozen=True, slots=True)
class Grant:
    """Long-lived proof of wallet control. BROWSER from SIWE (cookie, 7 d);
    APIKEY minted from a browser capability (shown once, no expiry)."""
    kind: GrantKind          # BROWSER | APIKEY (one byte)
    wallet: bytes
    key_id: bytes            # 8 random bytes; label-free, printed in audit rows
    epoch: int               # must equal PolicyLog.epoch(wallet) at exchange
    issued_at: int
    expires_at: int          # 0 = none

@dataclass(frozen=True, slots=True)
class Holding:
    wallet: bytes; balance_raw: int; block: int; observed_at: float

@dataclass(frozen=True, slots=True)
class Entitlement:
    features: Feature; holding: Holding | None; since_ok: int; reason: str   # reason for the largest refusal

class GateRefusal(Exception):
    status: int; reason: str; feature: str | None; detail: dict

# gate_policy.py --------------------------------------------------------------
@dataclass(frozen=True, slots=True)
class GatePolicy:
    """Owner-signed. thresholds are raw token units (uint256); 0 = any signed-in
    wallet; feature absent = disabled. token None = nothing is holdable."""
    token: bytes | None; decimals: int
    thresholds: Mapping[Feature, int]
    grace_s: int; version: int
    EIP712_TYPE = "GatePolicy(address token,uint8 decimals,uint256 tradeMin,uint256 lpMin,uint256 apiMin,uint256 flagsMin,uint32 graceSeconds,uint32 version,uint64 deadline)"
    EIP712_DOMAIN = {"name": "rhpools gate", "version": "1", "chainId": 4663}
```

Wire encoding of `Capability` and `Grant` (private to `gate_crypto.py`):
`<kind byte><struct payload>` → `base64url(payload) "." base64url(HMAC-SHA256(secret, kind||payload))`.
Capabilities are `rhc1_…`, grants `rhg1_…` (browser cookie) / `rhk1_…` (API
key). No JSON, no algorithm field, no header: nothing to confuse.

### Persistent state (gate.sqlite, own file, own connection, WAL)

```sql
CREATE TABLE audit_log (
  id INTEGER PRIMARY KEY,           -- monotonic; policy rows' id is NOT the policy version
  at REAL NOT NULL, kind TEXT NOT NULL CHECK (kind IN ('policy','epoch','key')),
  wallet BLOB, source TEXT NOT NULL,  -- 'eip712' | 'cli:<user>' | 'cap'
  payload TEXT NOT NULL,             -- JSON: the policy, or {key_id}, or {}
  signature BLOB, message TEXT       -- EIP-712 signature + canonical JSON for policy rows
);
CREATE INDEX audit_wallet ON audit_log(kind, wallet);
```

Everything is derived, never synced (per single-source-of-truth): current policy
= last `policy` row; `epoch(wallet)` = count of `epoch` rows for the wallet;
minted keys are `key` rows (audit only; the key itself is never stored). One
writer (the server process, or the CLI while the server is stopped or via the
same file with `busy_timeout`); the market DB is never touched.

Ephemeral in-process state, lost on restart by design: holding cache (30 s),
seen SIWE nonces (5 min ring), rate-limit buckets, active-stream counters, and
the boot nonce that scopes SIWE nonces to this process lifetime.

### Data flow

```
GET /api/gate/nonce  ──► Gate.nonce()          nonce = ts ‖ HMAC(boot_secret, ts)[:8]   (no storage)
POST /api/gate/siwe  ──► Gate.sign_in(msg,sig,host)
      parse EIP-4361 ─► domain==host, uri origin ∈ origins|loopback, chainId==4663,
      nonce mac+fresh+unseen ─► recover(personal_sign hash) or EIP-1271 eth_call
      ─► Grant(BROWSER, epoch=log.epoch(wallet)) ─► Gate.issue(grant, prior=None)
POST /api/gate/token ──► Gate.issue(grant, prior)
      verify grant mac/exp ─► epoch == log.epoch(wallet) ─► HoldingOracle.holding(wallet)
      ─► entitle(policy, holding, prior.since_ok, now) ─► Capability(policy_version, since_ok)
any gated request    ──► Gate.capability(headers) : mac ─► exp ─► policy_version   (no I/O)
SSE/WS loop          ──► Gate.still_valid(cap): exp, policy_version each iteration; `event: gate` then close
POST /api/gate/policy──► Gate.set_policy_signed(policy, sig): EIP-712 hash ─► recover == OWNER
      ─► version == current+1, deadline ≥ now ─► append policy row ─► every capability is now stale
```

`entitle` is the one pure function that encodes the rules:

```python
def entitle(policy: GatePolicy, holding: Holding | None, prior_since_ok: int, now: int) -> Entitlement:
    """Pure. token None or holding None (oracle failed) ⇒ Feature(0), reason token_unset|oracle_unavailable.
    ok = balance ≥ min(thresholds of features with threshold>0) … computed per feature:
      feature held if threshold == 0 or balance ≥ threshold
                   or (prior_since_ok and now - prior_since_ok ≤ policy.grace_s and it was held then)
    since_ok = now if any threshold>0 feature is held by balance, else prior_since_ok if inside grace, else 0."""
    raise NotImplementedError
```

Grace is thus a claim in the capability, not a table: a holder who sells keeps
exactly `grace_s` from the last ≥ observation, across restarts, in any client
that sends `prior` back. A fresh sign-in after selling gets no grace (correct:
grace protects continuity, not entry).

### Signatures (Python)

```python
# gate.py — the façade the Handler and CLI talk to. Deep: hides tokens, policy, oracle, DB.
class Gate:
    def __init__(self, *, secret: bytes, owner: bytes | None, db_path: Path,
                 rpc_call: Callable[[str, list], Any] | None, clock=time.time,
                 cap_ttl_s: int = 300, grant_ttl_s: int = 7 * 86400) -> None: ...
    def status(self) -> dict: """Public: token, decimals, thresholds, grace_s, policy_version, owner_set."""; raise NotImplementedError
    def nonce(self) -> dict: raise NotImplementedError
    def sign_in(self, message: str, signature: bytes, *, host: str, loopback: bool) -> tuple[Grant, Capability]: raise NotImplementedError
    def issue(self, grant_token: str, prior_token: str | None) -> Capability:
        """Exchange. Raises GateRefusal(401 invalid|expired|revoked) or (503 oracle_unavailable)."""; raise NotImplementedError
    def capability(self, token: str | None) -> Capability | None:
        """Hot path. None for absent; raises GateRefusal(401) for present-but-invalid so a
        stale cookie never silently degrades to anonymous."""; raise NotImplementedError
    def still_valid(self, cap: Capability) -> str | None: """None or 'expired'|'policy_changed'."""; raise NotImplementedError
    def mint_key(self, cap: Capability) -> tuple[str, Grant]: """Requires Feature.API. Audit 'key' row. Returns rhk1_ once."""; raise NotImplementedError
    def revoke_wallet(self, wallet: bytes, *, source: str) -> int: """Append 'epoch' row; return new epoch."""; raise NotImplementedError
    def set_policy_signed(self, policy: GatePolicy, signature: bytes, deadline: int) -> GatePolicy: raise NotImplementedError
    def set_policy_host(self, policy: GatePolicy, *, actor: str) -> GatePolicy: raise NotImplementedError
    def policy(self) -> GatePolicy: raise NotImplementedError
    def close(self) -> None: raise NotImplementedError

class RateLimiter:
    """Token bucket per capability.subject (key_id, or wallet for browser). In-memory, LRU-bounded (4096)."""
    def __init__(self, per_minute: int, burst: int) -> None: ...
    def allow(self, subject: bytes, cost: int = 1) -> int: """0 if allowed, else seconds until a token frees."""; raise NotImplementedError

class StreamBudget:
    """Concurrent streams per subject; context manager; raises GateRefusal(429) when exceeded."""
    def __init__(self, per_subject: int) -> None: ...
    def hold(self, subject: bytes) -> AbstractContextManager[None]: raise NotImplementedError

# gate_crypto.py — pure bytes in / bytes out. No I/O.
def encode_token(prefix: str, payload: bytes, secret: bytes) -> str: raise NotImplementedError
def decode_token(prefix: str, token: str, secret: bytes) -> bytes | None: """Constant-time compare; None on any defect."""; raise NotImplementedError
def pack_capability(cap: Capability) -> bytes: raise NotImplementedError      # struct ">20sBII8sIII" fixed 49 bytes
def unpack_capability(raw: bytes) -> Capability: raise NotImplementedError
def pack_grant(grant: Grant) -> bytes: raise NotImplementedError
def unpack_grant(raw: bytes) -> Grant: raise NotImplementedError
def personal_sign_hash(message: bytes) -> bytes: raise NotImplementedError    # keccak("\x19Ethereum Signed Message:\n" + len + msg)
def eip712_hash(domain: Mapping, primary_type: str, types: Mapping, message: Mapping) -> bytes: raise NotImplementedError
def recover_signer(digest: bytes, signature: bytes) -> bytes:
    """65-byte r‖s‖v with v ∈ {0,1,27,28}; normalizes v; low-s enforced; eth_keys NativeECCBackend."""; raise NotImplementedError

# gate_siwe.py — EIP-4361 ABNF parser + verification against the request's host.
@dataclass(frozen=True, slots=True)
class SiweMessage:
    domain: str; address: bytes; statement: str | None; uri: str; version: str; chain_id: int
    nonce: str; issued_at: str; expiration_time: str | None; not_before: str | None; resources: tuple[str, ...]
def parse_siwe(message: str) -> SiweMessage: """Strict; raises ValueError with the offending line name."""; raise NotImplementedError
def check_siwe(msg: SiweMessage, *, host: str, origins: frozenset[str], loopback: bool, now: int) -> None: raise NotImplementedError
def verify_eip1271(rpc_call, wallet: bytes, digest: bytes, signature: bytes) -> bool: """eth_call isValidSignature(bytes32,bytes) == 0x1626ba7e."""; raise NotImplementedError

# gate_holding.py — the oracle.
class HoldingOracle:
    """balanceOf(wallet) at latest via one JSON-RPC batch [eth_blockNumber, eth_call]. 30 s cache keyed by
    (token, wallet), LRU 4096, single-flight per wallet. Never touches the market store."""
    def __init__(self, rpc_call: Callable[[str, list], Any], *, ttl_s: float = 30.0, clock=time.monotonic) -> None: ...
    def holding(self, token: bytes, wallet: bytes) -> Holding: """Raises OracleUnavailable; caller fails closed."""; raise NotImplementedError
    def token_metadata(self, token: bytes) -> tuple[int, bytes]: """(decimals, code_hash) for status and policy sanity."""; raise NotImplementedError

# gate_policy.py — policy type, EIP-712 typed data, and the audit log.
def policy_typed_data(policy: GatePolicy, deadline: int) -> dict: """Exact eth_signTypedData_v4 JSON; served to the owner panel."""; raise NotImplementedError
def policy_from_message(message: Mapping) -> tuple[GatePolicy, int]: raise NotImplementedError
class PolicyLog:
    def __init__(self, path: Path) -> None: ...
    def current(self) -> GatePolicy: """Empty log ⇒ GatePolicy(token=None, version=0)."""; raise NotImplementedError
    def append_policy(self, policy: GatePolicy, *, source: str, signature: bytes | None, message: str | None) -> None:
        """Enforces version == current.version + 1 inside one transaction (idempotent replays are refused, not duplicated)."""; raise NotImplementedError
    def epoch(self, wallet: bytes) -> int: raise NotImplementedError
    def append_epoch(self, wallet: bytes, *, source: str) -> int: raise NotImplementedError
    def append_key(self, wallet: bytes, key_id: bytes) -> None: raise NotImplementedError
    def rows(self, limit: int = 200) -> list[dict]: raise NotImplementedError

# gate_cli.py — console script `rhpools-gate` (pyproject [project.scripts]).
def main(argv: list[str] | None = None) -> int: """show | set-policy | revoke-wallet | rotate-secret | audit."""; raise NotImplementedError
```

### Integration points in `lp_server.py`

```python
class Route(NamedTuple):
    resource: str | None; method: str; lane: str; freshness: ...; gated: str | None = None   # feature name

_GATE_ROUTES = {"/api/gate/status", "/api/gate/nonce", "/api/gate/me"}                        # GET
_GATE_POSTS = {"/api/gate/siwe", "/api/gate/token", "/api/gate/logout", "/api/gate/keys", "/api/gate/keys/rotate", "/api/gate/policy"}

class Handler(BaseHTTPRequestHandler):
    keyed_slots: threading.BoundedSemaphore        # separate lane for capability-bearing requests
    def _capability(self) -> Capability | None:
        """Authorization: Bearer rhc1_… wins; else cookie rhp_cap. Memoized per request. Also applies
        RateLimiter for the subject (raises GateRefusal 429) and selects keyed_slots in _bounded."""; raise NotImplementedError
    def _require(self, feature: str) -> Capability: raise NotImplementedError
    def _refuse(self, refusal: GateRefusal) -> None: """_json(status, {"error", "gate": {...}}, private=True)."""; raise NotImplementedError
    def _json(self, status, payload, *, retry=None, key=None, fresh=None, private: bool = False) -> None:
        """private=True: Cache-Control: private, no-store; Vary: Cookie, Authorization; NO Access-Control-Allow-Origin.
        private=False: exactly today's headers (golden)."""; raise NotImplementedError
    def _set_cookie(self, name: str, value: str, *, path: str, max_age: int) -> None:
        """HttpOnly; SameSite=Strict; Secure unless loopback; rhp_cap Path=/api, rhp_grant Path=/api/gate."""; raise NotImplementedError
    def _gate_get(self, path, query) -> None: raise NotImplementedError
    def _gate_post(self, path, payload) -> None: """Same-origin required (existing _same_origin) unless Bearer rhk1_ on /token."""; raise NotImplementedError
    def _ws(self, query) -> None:
        """GET /api/lp/ws with Upgrade: websocket. Requires Feature.API. websockets.server.ServerProtocol
        (sans-I/O, in-venv 15.0.1) over self.request; each SSE frame becomes one text frame."""; raise NotImplementedError
```

`_lp_stream`/`_workbench_stream` take `cap: Capability | None`; each loop
iteration calls `gate.still_valid(cap)` and, on a reason, writes
`event: gate\ndata: {"reason": …}\n\n` and returns. Memo keys for gated frames
add `int(cap.features)`. Anonymous streams run today's code path verbatim.

Runtime constructs `Gate` after `market` (needs the RPC factory:
`build_rpc_factory(args.gate_rpc_url, RpcError)("gate").call`). Host config:
`RHP_GATE_SECRET_FILE` (32 random bytes, 0600; absent ⇒ gate disabled, all
`/api/gate/*` 404), `RHP_GATE_OWNER` (pinned owner address; absent ⇒ signed
policy changes refused, CLI only), `RHP_GATE_DB` (default
`$RHP_DATA_DIR/gate.sqlite`), `RHP_GATE_RPC_URL` (default
`http://127.0.0.1:8547`), `--gate-rate-per-min 600 --gate-streams-per-subject 4
--gate-api-slots 8`. The systemd unit gains those `Environment=` lines; the
tunnel regex gains `/api/gate/(status|nonce|me|siwe|token|logout|keys|keys/rotate|policy)`,
`/api/lp/ws`, and `lp_gate` in the static alternation.

### Browser surface

`static/lp_gate.js` + `lp_gate.css`, loaded by `lp_terminal.html` after
`lp_terminal.js`; a `<span id="gate-strip" class="strip-state">` in the status
strip next to `#stream-state`, plus two `<dialog>`s (`#gate-keys`, `#gate-policy`).
States, in the header's existing token palette:

| state | text | class | trigger |
|---|---|---|---|
| no wallet | `WALLET —` (dim) | — | `!window.ethereum` |
| disconnected | `CONNECT` | `is-live` | click → `eth_requestAccounts` + chain 4663 check |
| connected, no cap | `SIGN IN` | `is-live` | click → nonce, personal_sign, /siwe |
| holder | `HOLDER trade lp api flags` | `is-good` | claims.features non-empty, since_ok fresh |
| grace | `GRACE 12:34` | `is-warn` | balance < min, since_ok inside grace; countdown |
| below | `BELOW MIN 10/1000 RHP` | `is-warn` | features empty, holding present |
| locked | `LOCKED` | `is-stale` | status.token null |
| stale | `SIGN IN` + toast | `is-stale` | 401 `policy_changed`/`expired` after a failed refresh |

Refresh runs 30 s before `expires` and on `event: gate`; the terminal's stream
reconnect logic is reused with the cookie. `KEYS` shows epoch, MINT (key printed
once with copy), ROTATE (confirm). `POLICY` renders only when
`claims.wallet == status.owner`. All fetches are same-origin with
`credentials: "same-origin"`; no token string is ever in JS memory except the
API key at mint time.

### Invariants encoded

- Anonymous ⇒ byte-identical: `_capability()` returning `None` is the only
  branch anonymous requests take, and `private=False` emits today's header set;
  the golden test diffs headers and bodies.
- No capability is minted without a fresh oracle read or an in-grace prior
  claim (type: `issue` is the only constructor path; `Capability` is frozen).
- Hot-path validity needs no I/O: HMAC, expiry, `policy_version` (in memory).
- Gated bodies are never shared-cacheable and never carry `ACAO: *` (`private=True`
  is forced whenever `cap is not None`).
- Policy changes are serialized by `version == current + 1` inside the log
  transaction: a replayed owner signature is refused, and every accepted change
  is an audit row with signer, signature, canonical message and source.
- The server holds an HMAC secret and (optionally) the owner's *address*; it
  never holds a private key, never signs chain messages, never broadcasts.
- Gate state never opens the market SQLite; `Gate` takes a path and an
  `rpc_call`, nothing from `LPMarketService`.

### What the design deliberately does not do

No per-request epoch check (revocation lands within `cap_ttl_s`); no key
listing (keys are label-free, rotation is per wallet); no refresh across a
secret rotation; no per-wallet server memory of grace (the client carries it;
programs that discard `prior` get instant revoke, documented).

## Test and verification harness

`tests/test_gate.py` (pure, fast; `FakeRPC` like `test_workbench_actions.py`):

- token codec round-trips, tamper/truncate/prefix-swap ⇒ `None`; constant-time compare.
- `entitle`: threshold 0, absent feature, token unset, oracle failure, grace
  boundary at exactly `grace_s`, since_ok carry and expiry, decimals irrelevant.
- SIWE parser against the EIP-4361 reference vectors; domain/uri/chain/nonce
  refusals; nonce replay within a process refused; signature from
  `cast wallet sign` recovers (v=27/28 normalization).
- EIP-712: `policy_typed_data` hash equals `cast --sign-typed-data`… concretely
  `cast wallet sign --data --from-file typed.json` with a throwaway key, and a
  non-owner signature is refused with an audit table unchanged.
- `PolicyLog`: version gap refused, concurrent appends serialize, epoch derives.
- Server: golden compare of every `_ROUTES` path + stream head with and without
  the gate constructed; gated request gets `private` headers and no ACAO; cookie
  flags; 429 after burst; stream closes with `event: gate` at forced expiry
  (`clock` injected).

`tests/test_gate_fork.py` (skipped unless `anvil` on PATH and `RHP_FORK_RPC` set; default `http://127.0.0.1:8547`):

1. `anvil --fork-url $RHP_FORK_RPC --port <free> --chain-id 4663`.
2. Stand-in token: `anvil_setCode(0x…d00d, STANDIN_ERC20)` where `STANDIN_ERC20` is the 67-byte
   runtime below (balanceOf reads mapping slot 0; decimals()=18; anything else reverts);
   balances via `anvil_setStorageAt(token, keccak(pad(wallet)‖pad(0)), amount)`.
3. Owner (anvil key 0) signs the EIP-712 policy with `eth_signTypedData_v4` through
   `cast wallet sign --data`; non-owner (key 1) refused; audit row present.
4. Holder (key 1) at threshold: SIWE via `cast wallet sign`; `/token` yields
   `api|flags`; mint key; SSE with key-derived capability streams.
5. `anvil_setStorageAt` balance below threshold; refresh with `prior` keeps
   features; `evm_setNextBlockTimestamp` past `grace_s` + injected clock; next
   refresh drops `api`; the open stream ends with `event: gate` at expiry and the
   reconnect is 403 `below_threshold`.

```
STANDIN_ERC20 = 0x60003560e01c806370a0823114601e578063313ce56714603857600080fd
                  5b600435600052600060205260406000205460005260206000f3
                  5b601260005260206000f3          # keccak 0xf08e4137…56c2
```

Browser proof (unit 1): open the terminal on the fork RPC with a wallet holding
key 1, walk CONNECT → SIGN IN → HOLDER → GRACE → BELOW, and inspect cookie
flags in devtools.

## Chain facts relied on

Verified today against `http://127.0.0.1:8547` (cast) and a private anvil fork:

- chain id 4663; head block 74,142,939 at probe time.
- `balanceOf(address)` (0x70a08231) and `decimals()` (0x313ce567) eth_calls work;
  USDG `0x5fc5…168` reports **decimals 6** (the ChainToken report's 18 was
  wrong; irrelevant to the gate but recorded).
- `ecrecover` precompile `0x…01` via `eth_call` recovers a `cast wallet sign`
  signature (kept as an optional cross-check only; primary recovery is in-process).
- `eth-keys` 0.7 pure-Python backend recovers in ~3 ms; wallets emit v ∈ {27,28},
  the library wants {0,1}: normalization is mandatory. Not in the venv today:
  add `eth-keys>=0.5,<1` to `dependencies`.
- anvil 1.7.1 forks the local Nitro node; `anvil_setCode`, `anvil_setStorageAt`,
  `anvil_mine`, `evm_setNextBlockTimestamp`, and pinned-block `eth_call` all work;
  the 67-byte stand-in ERC-20 above returns the set balance, decimals 18, reverts
  on `transfer`.
- `websockets` 15.0.1 in the venv exposes the sans-I/O `ServerProtocol`.

Assumed, not verified: EIP-1271 wallets exist on chain 4663 (path is optional);
Cloudflare honours `Cache-Control: private` at the tunnel (the design also sets
`no-store` so nothing depends on it); Permit2/UniversalRouter/Pons facts are
untouched by the gate.

## Tradeoffs accepted

- We accept revocation latency of one capability TTL (5 min) for a hot path with zero I/O.
- We accept that logout is cookie deletion, not server-side invalidation, in exchange for no session table; "sign out everywhere" is an epoch bump.
- We accept label-free keys and no key listing in exchange for never storing key material or key rows beyond an audit line.
- We accept that a program which drops `prior` loses grace, in exchange for grace that survives restarts and needs no per-wallet memory.
- We accept a 5-minute nonce replay window scoped to one process boot, mitigated by an in-memory seen-set, instead of a nonce table.
- We accept one new pure-Python dependency (`eth-keys`) over an RPC round-trip to the ecrecover precompile for every sign-in.
- We accept adding a new console script and four `Environment=` lines to the unit; the owner address is host config, not a policy field.

## Alternatives considered

- **Session table + opaque session id (candidate 1's family).** Hides less: every gated request pays a DB read and the gate needs its own writer discipline; exposes the same cookie surface to callers. Wins only on instant revocation, which the epoch/TTL pair covers well enough for a 5-minute window.
- **Raw API key on every request, oracle per request.** Simplest client story, but puts the RPC on the hot path (oracle down ⇒ API down) and makes rate limiting the only defence against balanceOf amplification. The exchange step keeps RPC calls to once per 5 min per client.
- **Grace as an in-memory `ok_since` map.** Fewer moving parts on the wire, but lost on restart and per-process, so a deploy during a holder's sale would instantly revoke: the exact case grace exists for.
- **JWT (RS256/HS256) with a JSON claims set.** Familiar, but adds an algorithm field and a JSON parser to the trust boundary for no gain; a fixed struct with one HMAC is smaller and cannot be downgraded.
- **Signing the capability with the owner's key (on-chain verifiable).** Would require the server to hold a key. Refused by the invariant.

## Open questions and risks

- Is a 5-minute revocation lag acceptable for `revoke-wallet`, or should the SSE/WS loop also poll `epoch` (one cheap SQLite read per minute per stream)?
- Should `flags` default to threshold 0 at launch so any signed-in wallet sees tags, as a funnel toward the token?
- EIP-1271: ship in unit 1 or defer until a contract-wallet user asks? It adds an RPC call to sign-in and a class of wallet we cannot test on the fork without deploying a 1271 stub.
- WebSocket: SSE already serves programs; is `/api/lp/ws` worth its tunnel entry and framing code in unit 2, or should it wait for a consumer?
- Key epoch is per wallet: does the owner want per-key revocation badly enough to store key ids (a second tiny table), or is "rotate everything" fine?

## Next implementation step

Write `gate_crypto.py` (token codec, personal_sign/EIP-712 hashing, recovery with v normalization) and its unit tests against `cast wallet sign` vectors, since every other module's boundary is a token or a signature.
