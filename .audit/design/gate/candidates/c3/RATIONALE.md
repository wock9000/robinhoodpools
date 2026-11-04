# Rationale, candidate 3 (unified credential)

## Why one credential

The assignment fixes the direction: SIWE mints a key, the browser holds it in an HttpOnly cookie, programs send it as a bearer, and entitlement belongs to the wallet. The design leans into it rather than softening it: there is one table, one `Principal` type, one `resolve()` and one `entitlement()`, and every gated surface (REST, SSE, WS, key minting, the owner policy route) is a caller of the same two functions. `via` on the principal is the only trace of transport, and it exists solely because the CSRF rule differs for cookies and bearers.

Interface depth: `Gate` hides EIP-712 hashing, EIP-4361 parsing, secp256k1 recovery, the 7702/1271 distinction, the balance cache and single-flight, the grace state machine, the rate limiter, and the SQLite schema behind ten methods. The handler learns none of it; it maps `GateRefusal` to a status and sets a cookie. A reader can trace a request from header to refusal in two files.

## Alternatives considered and rejected

1. **Session cookie + separate API-key table (two credential types).** Exposes two lookups, two expiry policies, and two revocation flows to the handler; every stream needs to know which it got. Hides nothing extra: both resolve to a wallet. Rejected for surface size; also the direction forbids it.

2. **Key-level entitlement (scopes baked into each key at mint).** Simpler refusal logic (no oracle on the hot path) but wrong under the product rule: when the owner raises a threshold or the holder sells, keys minted yesterday must stop working today. Scopes would have to be re-derived from the wallet anyway. Rejected: derive, don't sync.

3. **JWT/PASETO stateless tokens carrying the wallet.** Removes the credential table but makes revocation and per-key limits impossible without a denylist, i.e., a table. Also puts the wallet in a client-readable token. Rejected: the stateful key is smaller once revocation is required.

4. **ecrecover via the `0x01` precompile through `eth_call`, no new dependency.** Zero-dep and exact, but sign-in and policy application would fail whenever the node is down, and the CLI fallback exists precisely for degraded days. `eth-keys` pure-Python is small, from the same org as `eth-abi`/`eth-utils` already pinned, and was verified against `cast` output. Rejected on availability.

5. **Gate state inside the market SQLite.** One file, one backup. Rejected by the plan: the market writer is contended and 652 GB; gate writes are tiny and must not queue behind it.

6. **SSE only, no WebSocket.** Less code (no RFC 6455). Rejected because the assignment names WS for programmatic clients and browsers cannot add headers to `EventSource`/`WebSocket` anyway; with the cookie both work identically, and a server-push-only WS is ~150 lines.

7. **Short session + refresh token.** Better theft window, but a second credential shape and a client refresh loop in vanilla JS. Rejected in favor of revocable long-lived keys, listed as an open question.

8. **Grace implemented as "last balance ≥ threshold at the moment of drop plus timer".** Requires detecting the drop edge and storing a timer per wallet. The chosen rule (last observed qualifying time + grace) needs one timestamp per (wallet, feature), is edge-free, and covers sells, flaps, and RPC blips with the same line.

9. **Unsigned host-CLI override for policy.** Convenient when the owner's wallet is unavailable. Rejected: the done predicate says only an owner-signed message changes policy; the CLI stays a transport fallback, and `cast wallet sign --data --from-file` was verified to produce a matching signature offline.

## Facts that changed the design during grounding

- The three well-known anvil accounts carry EIP-7702 delegation on chain 4663. The SIWE verifier therefore treats `0xef0100‖addr` code as an EOA and the fork harness generates fresh keys.
- USDG is an ERC-1967 proxy; `anvil_dealERC20` cannot find its balance slot, so the harness funds test wallets by impersonating the WETH/USDG pool (verified).
- `_json` memoizes public bodies by publication key; any principal-aware response must bypass that path (`private=True`) or the anonymous cache would serve gated fields.
