# Programmability

This milestone aims to stabilize the execution model for chain operations.

## Proposal: sharded smart contracts

We wish to support notably user-defined assets and wrapped currencies from
external blockchains. (See the [Wormhole bridge](https://wormholenetwork.com/) of Solana
is used to bridge ERC-22 to SPL and also [this
note](https://medium.com/the4thpillar/ethereum-erc-20-four-to-solana-spl-four-token-bridge-manual-10c33e64030f).)

ERC-22-like smart contracts are typically co-locating the assets of every user in a map.
Here, we must shard such global state by each user chain.

This results in the following proposal:

* Every chain has

    - a unique address ("UID" in the Zef paper),

    - a group of owners (possibly all the validators),

    - an epoch number (for reconfiguration),

    - a table of published smart contract definitions (i.e. bytecode). NOTE: Publishing
      could be restricted to special chains.

    - a table of smart contract "local instances" controlled by this chain (with
      associated states in storage).

* Compared to a classical blockchain, the instantiation of a smart-contract corresponds
  to a collections of local instances.

* There are two kinds of local instances:

    - An admin instance is created when a smart contract definition is instantiated by the admin user (e.g.
      to create a "fresh" token).

    - User instances hold the portion of a smart contract state related to a particular
      admin instance and user chain.

* Local instances can call into (the local instances of) other smart contracts whenever
  they are located in the same user chain. Otherwise, cross-chain (async) messages
  are required.

## Proposal: cross-chain messaging

* Executing transactions in a chain may send asynchronous messages to other chains.

* We may support several types of messages:

    - State updates

    - User notifications

    - User subscriptions (and cancellation of subscription)

    - State queries (TBD)

* State updates are meant to modify the state (e.g. increase the balance, force a reconfiguration of validators)

    - First, updates go to the "inbox" of the chain.

    - Contrary to FastPay/Zef, received updates must be "accepted" (in particular ordered)
      by the receiver in a next transaction.

    - Some special updates can be marked as "mandatory": they must be included in the next
      transaction. (Use case: mandatory reconfigurations. However, this requires a
      multi-owner chain.)

* User notifications do not modify the (observable) chain state. They are meant for the
  owner(s) of the chain.

* Similarly, user subscriptions are meant to manage future user notifications. (Use cases:
  reconfiguration, smart contract updates, trading, etc)

* State queries (TBD)

    - May sample a particular public value of the chain state.

    - For consistency reasons, a state query must include the block height (i.e. version number)
      of the chain state to be queried.


## Proposal: certify state during validation

Current state:

* A Linera "chain" can be seen as a (small) blockchain regularly extended with a
  new (block of) transaction(s).

* New blocks may be picked by a single owner (original design of FastPay/Zef) or decided
  by more complex protocols (see multi-owner chains and mempool-driven chains).

    - In FastPay/Zef, a block proposal consists of a single transaction signed by the
      owner of the account. Certified blocks as valid are final. (That is, execution never
      fails later.)

    - In traditional blockchains, a block is made of many transactions (often picked from a
      shared mempool). There is no early/final validation. During the final execution,
      certain transactions may fail and be silently skipped. (They are however removed
      from the mempool.)

* Regarding the certification of chain states:

    - Traditional blockchains can certify the chain state (Merkle root hash) after
      executing each block.

    - In FastPay/Zef, this is not possible because asynchronous cross-chain updates
      may cause validators to (temporarily) disagree on the state of a chain.

Proposal:

* Several operations to the same chain could be "bundled" in the same (all or nothing) transaction.

* Allowing many transactions per block could be useful for mempool-driven chains.

    - For good measure, if failing transactions are included in a block (because of
      fairness obligations?) they should be explicitly tagged as "failing".

* To certify execution states, the result of executing a block could be summarized and
  certified at the time of the validation itself. (This is possible because we eliminated eventual
  consistency in the messaging above.)

## TODOs

* How to prevent "spam" in the inbox. (This issue already exists in Zef btw.)

* Timeouts? Oracles? Randomness?

* What kind of VM should we use?

    - EVM is probably not easy to adapt to our model

    - [Move](https://move-book.com/) is used by Aptos and Sui

    - [Wasmer](https://docs.wasmer.io/) is used by Cosmos in [CosmWasm](https://docs.cosmwasm.com/docs/1.0/)
      and [Near](https://docs.near.org/docs/develop/contracts/rust/near-sdk-rs#)

