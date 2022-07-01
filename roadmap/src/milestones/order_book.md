# Decentralized Finance

Decentralized trading is the main application targeted in the MVP.
Other DeFi applications may also be considered.

## Automated Market Making

- The liquidity pool can be sharded (the sharding mechanism of the pool does
  not have to match the sharding used on the Linera side).
- "Real time" reporting of trades that can be used to update some price oracles
  and so provide "smart" rebalancing functionalities to the shards.

## Centralized matchmaker + distributed settlement

Smart settlement with external matching.
1. Use a central exchange platform (or multiple ones, e.g. liquidity pools).
2. Market participants first submits their instructions (order add, cancel,
   modify, ...) to the committee so that they are notarized and some stake is
   withheld.
3. The participant then submit the instructions + certificate to the *matchmaker*.
4. The notarized instruction authorizes the matchmaker to carry the trade.
5. The matchmaker is responsible for sequencing these instructions. Based on
   the sequencing, other systems can evaluate the state of the order book and
   print the resulting trades etc.
6. Once a trade is done, the matchmaker should lead the interaction with the
   committee to perform the actual swap.
7. Some safety guarantees are handled by such downstream systems.
    - Respecting the limit order details.
    - Maybe using some oracles refreshed regularly to enforce some price
      bounds.
    - Some front running/price improvement checks?

## Distributed matchmaker

This setting would only replace the centralized matchmaker with a distributed
version (steps 3 to 6 in the previous section).
- This seems to require a BFT consensus at least for instruction sequencing.
- This could be DAG based, e.g. [BullShark](https://arxiv.org/pdf/2201.05677.pdf),
  [All you need is DAG](https://arxiv.org/pdf/2102.08325.pdf).
- The advantage would be a fully distributed process, the drawback that matching
  would have the additional latency of the BFT consensus.
- Step 7 might not be totally necessary but good to have? Some parts of these
  should ensure some of the checks.

## Comparison with Serum

[Serum](https://www.projectserum.com/) provides exchange like trading to the
[Solana blockchain](https://solana.com/).

- The [Serum Roadmap 2.0](https://projectserum.medium.com/serum-roadmap-2-0-cc4f0405501)
  provides details on the current status of the project.
- Serum provides an on-chain *centralized order-book* (CLOB) where market
  makers are involved.
- A technical introduction to the Serum DEX: [google-docs](https://docs.google.com/document/d/1isGJES4jzQutI0GtQGuqtrBUqeHxl_xJNXdtOv4SdII/edit).
  This describes the interaction with the CLOB (placing/cancelling orders), getting
  the resulting events as well as the settlement process.
- [Serum whitepaper](https://assets.website-files.com/61382d4555f82a75dc677b6f/61384a6d5c937269dbed185c_serum_white_paper.88d98f84.pdf).
- Serum uses the SRM and MSRM tokens for governance. Fees are based on the amount of
  SRM that the participant owns (taker: between 3bps and 4bps, maker: 0bps). More
  details on the tokens and the related economics on the [projectserum website](https://www.projectserum.com/serum-token-summary).
- There is a large ecosystem of Serum based projects.
  - [Raydium](https://raydium.io/) provides some AMM functionality that can still
    benefit from the CLOB liquidity.
  - [Wormhole](https://solana.com/wormhole) provides some bridges with other
    chains to easily transfer tokens and other informations.

## Open questions
- Maybe the committee should only certify a hash commitment of the instructions
  so as not to allow front running of the orders by malicious validators.
- Should market makers run their own matchmakers? Having a centralized version
  would provide better liquidity but market makers would be slower to refresh their
  orders.
- If multiple matchmakers exist in parallel, maybe there could be some order
  routing service to transfer orders across the different makers.
- What order types do we want to support: limit orders at least, maybe pegged
  orders to some oracles?
- Bridges with ETH and/or BTC?
