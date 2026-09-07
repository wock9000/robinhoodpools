# Security Policy

## Project status

The standalone application is publicly distributed under AGPL-3.0-only. Report vulnerabilities privately; never include credentials or production user data in public reports.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting flow under **Security → Report a vulnerability** if it is enabled for this repository. Do not open a public issue, pull request, discussion, or commit containing vulnerability details.

If private vulnerability reporting is not enabled, ask a repository administrator to provide an existing private reporting path without including sensitive details in that request. This project does not publish a security email address; do not guess one.

In the private report, include the affected version or commit, impact, prerequisites, minimal reproduction, and any suggested mitigation. Remove credentials, private keys, seed phrases, personal data, and production database contents from the report. Coordinate disclosure with the repository owner while the report is under review.

## Secrets and transaction safety

Robinhood Pools must never receive or store a wallet private key or seed phrase. The default service is read-only with respect to the chain: it observes, indexes, previews, and simulates, but does not sign or broadcast. The optional transaction-preparation mode produces unsigned data only and must not become a server-signing path.

Treat credential-bearing RPC URLs as secrets. Store them outside the repository in mode-`0600` files with one URL per line and refer to them through the appropriate `LP_RPC_*_URL_FILES` or `RHP_RPC_URL_FILES` environment variable. Do not put them in command-line arguments, source, `.env` files, tests, browser code or storage, logs, screenshots, or reports. Rotate a credential immediately if it is exposed.

Keep the HTTP listener on loopback by default. Binding to a non-loopback address or authorizing an additional browser origin does not add authentication, TLS, request filtering, or host-level access control; those controls must be designed and operated separately.

## Sensitive local state

The SQLite index can reveal queried owners and locally retained chain-derived state even though its inputs are public. Give one running service process exclusive ownership of a database, restrict local filesystem access, and stop the process before backups or migrations. Never attach a live or production database to a report or commit it to the repository.

The HTTP process being reachable does not prove the index is current. Consumers and operators must assess `/api/lp/status` freshness, lag, coverage, and provider state independently of serving availability.
