# Rationale: why stateful sessions and hashed keys

The direction was assigned; this file records the shapes inside that direction
that were weighed, and the neighbouring shapes rejected, judged on interface
depth (what the caller must know versus what the module hides).

## Load-bearing choices

**Entitlement is derived per request, never stored in the credential.** A
session row or key row maps to a wallet and nothing else. Holding thresholds,
grace, and policy version live in one place (`GatePolicy` + `Holding`) and
`entitle()` is a pure function of the three. A holder who sells is cut off by
the next oracle refresh plus grace, with no revocation list, no token
reissue, and no "what version of the policy was this key minted under"
question. The cost is a cache lookup per request; the gain is that there is
exactly one place where "is a holder" is decided.

**Two caches, both keyed by hash, both TTL-bounded, one DB behind them.**
Session/key lookup and holding lookup are both `dict` reads on the hot path.
The gate DB is read only on a miss and written only on identity events and on
observation change. The market writer is never touched, satisfying the
constraint by construction rather than by discipline.

**Owner authority is a pinned address plus an EIP-712 struct, verified from
the server's own canonical encoding.** The client never controls `types` or
`domain`; it only supplies `message` fields and a signature. `version ==
head+1` gives replay protection and total order; `issued_at` bounds clock
skew. The host CLI writes the same table with a different `signer`, so the
audit log has one shape for both paths and the running server observes
either through `PRAGMA data_version`.

**Transport stays outside `Gate`.** `Gate` takes `email.message.Message`
headers and returns domain types; `GateHttp` and the two sinks are the only
code that knows about handlers, cookies, SSE lines, or WebSocket frames.
This keeps the test surface pure (policy, SIWE, entitle) and the HTTP tests
thin.

## Alternatives rejected

### Stateless signed cookies / JWT carrying wallet and features

Hides less than it seems: the server still has to hold a holding cache and
re-evaluate per request, so the token would only carry the wallet, which a
32-byte random id carries with fewer failure modes (no key rotation, no
algorithm confusion, no expiry-vs-revocation mismatch). Revocation of a leaked
key would need a denylist, which is state. Lost on depth: same public surface,
more caller-visible rules.

### `balanceOf` per request (no oracle cache)

Simplest to reason about and wrong twice: an RPC per REST hit puts the public
site's load onto the local node, and a holder mid-sell would flap between
200 and 403 within one block. The 30 s cache with `qualified_at` grace is the
minimum state that makes both problems disappear.

### Gate tables inside the market SQLite

One file, one store class, one connection pool. Rejected by the hard
constraint: the market writer is contended and 652 GB; a sign-in must not
wait behind an accounting pass, and a schema migration on that file is a
multi-hour operation.

### A separate auth/gate process in front of rhpools (reverse proxy)

Would keep `lp_server.py` untouched. Lost because the tunnel allowlist,
CSP, `_same_origin`, and the memo/ETag machinery all live in this process;
a proxy would have to re-derive the private/no-store and memo-key rules to
avoid leaking tagged bodies, duplicating the exact knowledge the gate must
own. Two processes also double the operational surface for one owner.

### `eth-account` for signature recovery

Gives `recover_message`/`recover_typed_data` for free. Pulls bitarray, ckzg,
hexbytes, pydantic, rlp, eth-keyfile and more into a stdlib-flavoured
service. `eth-keys` alone (deps already locked) plus ~60 lines of EIP-712
hashing that were verified against `cast` is the smaller and more auditable
surface.

### asyncio `websockets.serve` on a second port for the WS stream

Battle-tested server, but it means a second listener, a second tunnel
ingress, a second place to enforce the gate, and asyncio inside a process
built on threads. The sans-I/O `ServerProtocol` on the existing handler
socket was verified to work in ~40 lines and keeps one auth path.

### Key-time entitlement (mint checks holding; key is then unconditional)

Simplest for API consumers (a key either works or is revoked). Violates the
done predicate: a holder who sells must lose access after grace without
anyone revoking anything. Use-time evaluation is required; the mint-time
check is kept only so non-holders cannot pre-mint.

### Grace as a per-wallet timer in memory only

Loses grace on restart, which is exactly when it matters most (deploys
happen while people are selling). Persisting `qualified_at` per feature on
the holding row costs one small upsert per wallet per 30 s at most.

### CSRF tokens (double-submit) for gate POSTs

Adds a header the front-end must plumb into every fetch. The existing
`_same_origin` Origin check, `SameSite=Strict`, and JSON-only bodies (which
force a preflight that the unchanged OPTIONS handler refuses for POST) are
three independent defences already; a fourth adds surface without hiding
anything.

### Extending the OPTIONS preflight to allow `Authorization` on all routes

Would let cross-origin browser apps use keys. Changes the bytes of an
existing anonymous response, which the plan forbids, and encourages putting
keys into third-party front-ends. Keys are for programmatic clients; browsers
use the cookie.
