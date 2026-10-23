# Security Policy

## Project status

The standalone application is publicly distributed under AGPL-3.0-only. Report vulnerabilities privately; never include credentials or production user data in public reports.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting flow under **Security → Report a vulnerability** if it is enabled for this repository. Do not open a public issue, pull request, discussion, or commit containing vulnerability details.

If private vulnerability reporting is not enabled, ask a repository administrator to provide an existing private reporting path without including sensitive details in that request. This project does not publish a security email address; do not guess one.

In the private report, include the affected version or commit, impact, prerequisites, minimal reproduction, and any suggested mitigation. Remove credentials, private keys, seed phrases, personal data, and production database contents from the report. Coordinate disclosure with the repository owner while the report is under review.

## Secrets and transaction safety

Robinhood Pools must never receive or store a wallet private key or seed phrase. The default service is read-only with respect to the chain: it observes, indexes, previews, and simulates, but does not sign or broadcast. The optional transaction-preparation mode produces unsigned direct-pool remove and collect data only and must not become a server-signing path. MiniRouter2 `0x5295e633dfb504298d4a1896ba0738acb6c89e6a` add-liquidity execution is retired as unsafe; revoke remaining token allowances to it, and do not sign transactions from earlier add or approval quotes.

Treat credential-bearing RPC URLs as secrets. Store them outside the repository in mode-`0600` files with one URL per line and refer to them through the appropriate `LP_RPC_*_URL_FILES` or `RHP_RPC_URL_FILES` environment variable. Do not put them in command-line arguments, source, `.env` files, tests, browser code or storage, logs, screenshots, or reports. Rotate a credential immediately if it is exposed.

Holder trading and liquidity management keep that boundary. The server builds each transaction (Universal Router swaps, V3 position-manager and V4 PositionManager calls), simulates the exact bytes from the user's address, and hands them to the user's wallet to sign and send. It holds no key and never broadcasts. Targets are pinned by address and runtime code hash, checked at startup and before every prepare; a mismatch disables trading. Every swap carries a minimum output and a deadline, every liquidity change carries amount bounds, and Permit2 allowances are exact and expire with the quote. The 75 bps rhpools fee is paid inside the same transaction to the recipient set in host configuration (`RHP_TX_FEE_RECIPIENT`); without it trading reports disabled. Adds to Pons pools are refused because the hook keeps every swap fee.

Keep the HTTP listener on loopback by default. Binding to a non-loopback address or authorizing an additional browser origin does not add authentication, TLS, request filtering, or host-level access control; those controls must be designed and operated separately.

## Sessions, API keys and the holder gate

Wallet sign-in is EIP-4361: the server verifies a signature and never receives, stores, or produces one. `eth-keys` is used for recovery only; there is no signing code path. Sessions and API keys are one credential type whose secret is shown once and stored only as a SHA-256 hash; browsers keep it in the `__Host-rhp_session` cookie (`HttpOnly; Secure; SameSite=Strict`), scripts send it as a bearer. Keys can be minted from a browser session only, so a leaked bearer cannot spawn more keys. Cookie-authenticated POSTs require a same-origin `Origin`. Entitlement belongs to the wallet (on-chain balance, 30 s cache, grace window), never to the key, so revocation and threshold changes take effect at the next request or stream iteration and the gate fails closed when the balance is unknown.

Gate state lives in its own SQLite file (`gate.sqlite`, default next to the market database), never inside the market store, and nothing under the gate imports the market code. The policy is changed only by an EIP-712 message signed by the owner address pinned in host configuration (`RHP_GATE_OWNER`); the web route and the `rhpools-gate` CLI share one apply path, and every accepted or refused change writes an audit row with the recovered signer. Do not put the owner private key on the host: sign with a hardware or browser wallet and submit the signature.

## Sensitive local state

The SQLite index can reveal queried owners and locally retained chain-derived state even though its inputs are public. Give one running service process exclusive ownership of a database, restrict local filesystem access, and stop the process before backups or migrations. Never attach a live or production database to a report or commit it to the repository.

The HTTP process being reachable does not prove the index is current. Consumers and operators must assess `/api/lp/status` freshness, lag, coverage, and provider state independently of serving availability.
