# Decentralized Trading

This is the main application targeted in the MVP.

## Automated Market Making

- The liquidity pool can be sharded (the sharding mechanism of the pool does
  not have to match the sharding used on the zef account side).
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

## Open questions
- Maybe the committee should only certify a hash commitment of the instructions
  so as not to allow front running of the orders by malicious validators.
- Investigate what serum provides.
- Should market makers run their own matchmakers? Having a centralized version
  would provide better liquidity but market makers would be slower to refresh their
  orders.
- If multiple matchmakers exist in parallel, maybe there could be some order
  routing service to transfer orders across the different makers.
- What order types do we want to support: limit orders at least, maybe pegged
  orders to some oracles?
