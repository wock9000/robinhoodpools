# Gate arena synthesis

Base: candidate 3 (one credential type). Cross-judge (reviewer) scored C1 23,
C3 23, C2 16 and recommended C3. My rubric pass agreed: C3's gaps are local and
swappable; C1's two credential tables and unsigned CLI authority are structural.

## Grafts

| From | Graft | Why |
|---|---|---|
| C1 | Oracle keeps the last Holding only while `observed_at` is within `grace_s`; otherwise balance is unknown and gated features drop | C3 kept a cached balance of any age on RPC failure, freezing a seller's pre-sale balance through an outage |
| C1 | `last_ok_at` rows carry `policy_version`; rows from older versions are ignored | A threshold raise must not be bypassed by grace earned under the old policy |
| C1 | `__Host-rhp_session` cookie; existing-route OPTIONS bytes unchanged, `Authorization` allowed only on `/api/gate/*` and `/api/v1/*` | Byte identity and no invitation for third-party front-ends to embed keys |
| C1 | Sign-in limited to 5/min per IP and 60/min per process; nonces in memory, not a DB row per GET | Pure-Python ECDSA is ~9 ms; unauthenticated writes are a DoS lever |
| C1 | Only a cookie principal (browser session) may mint keys | A leaked bearer must not spawn more keys |
| C1 | websockets sans-I/O `ServerProtocol` instead of hand-rolled RFC 6455 | Already a dependency; verified by C1 and C2 |
| C2 | Stand-in ERC-20 installed with `anvil_setCode` (67-byte runtime) and balances set with `anvil_setStorageAt` | Deterministic; no dependence on live pool state at fork head |
| C1 | Golden recorded once at snapshot `1b00978`: every existing GET/HEAD/OPTIONS route and the first SSE frames | Proves predicate 1 against the deployed baseline, not against the new code |
| own | An invalid `rhp_`-prefixed bearer returns 401; any other `Authorization` value is ignored | A script with a revoked key must be told; unrelated headers keep anonymous bytes |

Kept from C3: one `Principal`, one `resolve()`, one `entitlement()`; host CLI
submits an owner-signed message through the same `apply_policy` (no unsigned
override); EIP-7702 designator `0xef0100‖addr` treated as an EOA; ERC-1271 only
for real contract code; fresh non-7702 keys in the harness (anvil accounts 0-2
carry 7702 code on 4663).

## Rejected

- C2 grant→capability exchange and client-carried grace: every client
  implements a refresh protocol, one `since_ok` leaks features across
  thresholds, and refusals go unaudited.
- C1 separate session and api_key tables: two lookups, two revocation flows,
  no extra capability.
- C3 key-mints-key: removed (see grafts).

## Verification

Pending implementation. Unit 0 proof is the test list in DESIGN.md plus the
grafted golden and fork harness.
