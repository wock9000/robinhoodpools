# Rationale: stateless capabilities

## Why this shape

The gate has exactly one fact that must be durable and owner-controlled: the
policy. Every other fact the plan lists as state (Session, ApiKey, Holding,
Entitlement) is either a *claim* the server can sign and hand back, or a cache.
Signing claims turns the hot path into `HMAC verify + two integer compares`,
with no reader on any database, which is the property the contended-writer
constraint and the "gate checks never touch the market writer" invariant are
really asking for. It also makes the gate trivially restart-safe: nothing to
recover, nothing to migrate, nothing to prune.

The two-tier token split (grant → capability) exists because the two questions
"is this wallet who it says it is" and "is this wallet entitled right now" have
different lifetimes. Wallet control is proven once (SIWE or key mint) and can be
trusted for days; entitlement is a chain read that goes stale in seconds. A
long-lived grant that must be *exchanged* for a short-lived capability lets the
oracle be consulted once per client per TTL instead of per request, while the
capability's TTL bounds how stale an entitlement can be. API keys and browser
sign-ins are the same grant type with a different delivery channel (bearer vs.
HttpOnly cookie), so there is one exchange function, one refusal vocabulary and
one test surface.

Grace lives inside the capability (`since_ok`) because the alternative homes
all fail the plan's own scenario. A table needs a writer; an in-memory map
forgets across a deploy, which is precisely when a holder mid-sale would be
revoked. A signed claim the client returns on refresh survives both, costs no
storage, and cannot be forged or extended by the client because the server
recomputes it from the prior claim and the current read.

Policy version inside the capability is the global revocation switch: the
owner's signed change invalidates every outstanding capability at once, without
enumerating them. Wallet key epoch is the per-wallet switch, checked at
exchange time so the hot path stays I/O-free. Secret rotation is the nuclear
switch. Three levers, no lists.

## Alternatives considered and rejected

1. **Session table with opaque ids (HttpOnly cookie → row).** Standard, and it
   gives instant revocation and a keys list. Rejected because it makes the gate a
   second stateful service with its own writer, lookup on every gated request,
   pruning, and restart recovery, in exchange for revocation latency that the
   5-minute TTL already bounds. Interface depth is equal on the browser side and
   worse on the server side: the Handler would need a store handle and an
   expiry sweeper.
2. **API key used directly on every request, oracle read per request.** The
   simplest curl story. Rejected because it puts the RPC on the hot path (an RPC
   outage becomes an API outage rather than a refresh failure), invites
   `balanceOf` amplification under a burst of keyed requests, and gives grace no
   natural home. The exchange step costs clients one extra POST per 5 minutes.
3. **Grace as an in-memory `wallet → last_ok` map.** Fewer wire fields. Rejected
   for losing the memory on restart and per process; a deploy during a sale is
   the exact case grace exists for.
4. **JWT with JSON claims (HS256).** Familiar tooling. Rejected because the
   algorithm header and JSON parsing sit on the trust boundary for no benefit;
   a versioned fixed-width struct with one HMAC is smaller, cannot be
   downgraded, and needs no library.
5. **Nonce table for SIWE.** Spec-literal single-use nonces. Rejected in favour
   of HMAC-timestamped nonces scoped to the process boot plus a bounded seen-set:
   replay is confined to one 5-minute window in one process lifetime, and the
   attacker who could replay already holds the response. No writer needed.
6. **ecrecover through the local node's precompile instead of `eth-keys`.**
   Verified to work and stdlib-only. Rejected as the primary path because
   sign-in and policy changes would then depend on RPC availability, and it adds
   a network round trip per signature. Kept as an optional cross-check in tests.
7. **Per-key revocation with a stored key list.** Gives labels and "revoke this
   one". Rejected for this candidate to keep the persistent state to one
   append-only audit table; "rotate everything for this wallet" is the epoch
   bump. Flagged as an open question because it is the cheapest thing to add
   later (one table, no hot-path change).
8. **WebSocket via a second asyncio server on another port.** Rejected: the
   tunnel routes one origin and the CSP is `connect-src 'self'`. The sans-I/O
   `websockets.server.ServerProtocol` already in the venv lets the same Handler
   thread speak WS on the same port with the same stream generator.
9. **Owner address as a policy field.** Rejected: the owner must be pinned
   outside anything the web can change; it is host config
   (`RHP_GATE_OWNER`), exactly as the plan states.

## Red-flag screen

- Shallow module: `Gate` is deep; the Handler calls one method per concern
  (`capability`, `issue`, `sign_in`, `mint_key`, `set_policy_signed`) and never
  sees tokens, HMACs, the oracle, or the DB.
- Information leakage: wire encodings are private to `gate_crypto.py`; cookies
  and headers are private to the Handler; storage schema is private to
  `PolicyLog`.
- Temporal decomposition: modules are by knowledge (crypto, SIWE grammar,
  policy+audit, oracle), not by request phase.
- Pass-through: `Handler._require` adds policy (feature check, refusal
  mapping); `_capability` adds rate limiting and lane selection; neither
  forwards unchanged.

## What must be true for this to hold up

- The HMAC secret file is 0600 and outside the repo; its rotation is documented
  as "everyone signs in again".
- Gated responses are always `private, no-store` and never `ACAO: *`; the
  golden test is the guard for the anonymous byte-identity.
- `entitle` stays pure; if implementation wants to reach for the oracle or the
  log inside it, the sketch is wrong and should be reopened.
