# Coin-fee rebates: design options and risk (deferred)

Status: deferred by the owner until legal review. Nothing below is built.

## Where the money comes from

The token is a Pons V2 launch. Every swap in its pool pays a 100 bps Pons hook
fee plus the creator tax chosen at launch (0-500 bps on live pools; 0, 100 and
200 are most common). The creator tax accrues in the hook and a Pons operator
pushes it to the creator address. There is no on-chain claim rhpools could
redirect, so any rebate is a payment the owner makes from the owner wallet.

## Two shapes

| | Usage rebate | Holder distribution |
|---|---|---|
| Who gets paid | wallets that traded through rhpools, pro-rata to rhpools fee paid | every holder, pro-rata to balance |
| Data needed | rhpools fills (already recorded per trade by the receipt reconciler) | balance snapshots |
| How it reads | a volume discount on a service | a dividend on the token |
| Regulatory exposure | lower | highest; the construct apollo's own docs refused without legal review |

## Mechanics if it ships

- Weekly ledger: sum of `rhpools_fee` per wallet from confirmed fills; the
  rebate pool is 10% of creator tax received that week.
- Payout: the server computes the batch and returns an unsigned multicall of
  transfers; the owner signs it in a wallet. The server still signs nothing.
- A Merkle claim contract would let users pull their share, but it is a new
  contract holding funds and needs an audit first.

## Recommendation

Launch with access utility only (trading, liquidity, API and streams, tags).
If rebates follow, use the usage-rebate shape and the owner-signed batch
payout, after counsel reviews the wording and the token's marketing.
