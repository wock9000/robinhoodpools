# Provenance and licensing

This repository was extracted as a fresh-history, public-data-only Robinhood Chain liquidity-pool application. It intentionally excludes the source repository's Git history, private operator routes, signing material, cached census data, identity maps, and deployment credentials.

The repository owner confirmed redistribution rights and approved public release under **AGPL-3.0-only** on 2026-09-07. See the root `LICENSE`. The private source application and its history are not part of this release.

The integer TickMath constants and rounding semantics used by `src/rhpools/lp_math.py` are documented in the MIT-licensed Uniswap v4-core `TickMath.sol` and `SqrtPriceMath.sol`, pinned at commit `46c6834698c48bc4a463a86d8420f4eb1d7f3b75`. The local implementation uses Python integer arithmetic rather than vendoring the Solidity contracts. Source links and the MIT notice are retained in `THIRD_PARTY_NOTICES.txt`.

Deployment addresses and ABI selectors describe public on-chain interfaces. Installed Python dependencies retain their own licenses. The software license does not relicense external data feeds or grant access to keyed providers; the Apollo and RH Trenches adapters retain source attribution and coverage boundaries, and the paid FomoAPI feed is not bypassed.

The runtime prepares unsigned transaction data only when explicitly enabled. It does not store private keys, sign transactions, or broadcast transactions.
