# Merits of the Zefchain Protocol

## Expected Key Benefits

* Linear scalability
    - Validators can upscale/downscale to follow any thorough

* High performance
    - Low latency
    - Cost efficiency

* Low MEV
    - No mempool sniping
    - Front-running by miners limited thanks to low-latency (+ TBD ?)

* Usability
    - Predictable fees
    - Clients can query validators directly
    - Fast and cheap order book enables familiar trading activity (e.g. limit orders)

## Challenges

### For validator adoption

* Physical requirements for scalability / elasticity

    - Until decentralized cloud is a thing (see e.g. [Akash](https://akash.network/)), we
  will develop easy-to-deploy software for existing cloud providers (AWS, Azure, Google Cloud, etc).

    - See also [Infura](https://infura.io)

* Technical novelty / complexity

    - This requires great documentation, runbooks, and high-quality clients.

### For user adoption

* Perceived lack of decentralization due to limited number of validators (say <= 50)

    - We may have to embrace the label "CeDeFi" (see Binance Smart Chain).

    - Supporting many more validators may possible with more complex networking model, no
      user<->validator interactivity, and/or probabilistic sampling similar to Avalanche.

    - Pushing execution and storage to clients using ZK proofs could also be the future by
      making possible to handle many more shards on the same physical host (see [Verkle
      tree](https://vitalik.ca/general/2021/06/18/verkle.html)).

    - We could also pivot and become a linearly scalable ZK-rollup of Ethereum with well
      studied liveness and fairness guarantess.

* Generality / Programmability to allow high quality satellite projects (in addition to our DEX)

    - We are intentionally not following the single-threaded execution model of smart contracts.

    - We can still host arbitrary computation during execution (just with a visibility limited to the current account/subchain)

    - User<->validator interations do not need to run deterministic code (e.g. user-defined oracles possible).

    - See also Cardano's UTXO-based smart for a possible alternative model.

### For market-maker adoption

* Requires a more active role (and/or an intermediate) compared to being a liquidity provider in an AMM

* Fierce competition on yield?
