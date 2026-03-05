# Microchains

This section provides an introduction to microchains, the main building block of
the Linera Protocol. For a more formal treatment refer to the
[whitepaper](https://linera.io/whitepaper).

## Background

A **microchain** is a chain of blocks describing successive changes to a shared
state. We will use the terms _chain_ and _microchain_ interchangeably. Linera
microchains are similar to the familiar notion of blockchain, with the following
important specificities:

- An arbitrary number of microchains can coexist in a Linera network, all
  sharing the same set of validators and the same level of security. Creating a
  new microchain only takes one transaction on an existing chain.

- The task of proposing new blocks in a microchain can be assumed either by
  validators or by end users (or rather their wallets) depending on the
  configuration of a chain. Specifically, microchains can be _single-owner_,
  _multi-owner_, or _public_, depending on who is authorized to propose blocks.

## Cross-chain messaging

In traditional networks with a single blockchain, every transaction can access
the application's entire execution state. This is not the case in Linera where
the state of an application is spread across multiple microchains, and the state
on any individual microchain is only affected by the blocks of that microchain.

Cross-chain messaging is a way for different microchains to communicate with
each other asynchronously. This method allows applications and data to be
distributed across multiple chains for better scalability. When an application
on one chain sends a message to itself on another chain, a cross-chain request
is created. These requests are implemented using remote procedure calls (RPCs)
within the validators' internal network, ensuring that each request is executed
only once.

Instead of immediately modifying the target chain, messages are placed first in
the target chain's **inbox**. When an owner of the target chain creates its next
block in the future, they may reference a selection of messages taken from the
current inbox in the new block. This executes the selected messages and applies
their messages to the chain state.

Below is an example set of chains sending asynchronous messages to each other
over consecutive blocks.

```ignore
                               ┌───┐     ┌───┐     ┌───┐
                       Chain A │   ├────►│   ├────►│   │
                               └───┘     └───┘     └───┘
                                                     ▲
                                           ┌─────────┘
                                           │
                               ┌───┐     ┌─┴─┐     ┌───┐
                       Chain B │   ├────►│   ├────►│   │
                               └───┘     └─┬─┘     └───┘
                                           │         ▲
                                           │         │
                                           ▼         │
                               ┌───┐     ┌───┐     ┌─┴─┐
                       Chain C │   ├────►│   ├────►│   │
                               └───┘     └───┘     └───┘
```

The Linera protocol allows receivers to discard messages but not to change the
ordering of selected messages inside the communication queue between two chains.
If a selected message fails to execute, the wallet will automatically reject it
when proposing the receiver's block. The current implementation of the Linera
client always selects as many messages as possible from inboxes, and never
discards messages unless they fail to execute.

## Chain ownership semantics

Active chains can have one or multiple owners. Chains with zero owners are
permanently deactivated.

In Linera, the validators guarantee _safety_: On each chain, at each height,
there is at most one unique block.

But _liveness_—actually adding blocks to a chain at all—relies on the owners.
There are different types of rounds and owners, optimized for different use
cases:

- First an optional _fast_ round, where a _super owner_ can propose blocks that
  get confirmed with very particularly low latency, optimal for single-owner
  chains with no contention.
- Then a number of _multi-leader rounds_, where all _regular owners_ can propose
  blocks. This works well even if there is occasional, temporary contention: an
  owner using multiple devices, or multiple people using the same chain
  infrequently.
- And finally _single-leader rounds_: These give each regular chain owner a time
  slot in which only they can propose a new block, without being hindered by any
  other owners' proposals. This is ideal for chains with many users that are
  trying to commit blocks at the same time.

The number of multi-leader rounds is configurable: On chains with fluctuating
levels of activity, this allows the system to dynamically switch to
single-leader mode whenever all multi-leader rounds fail during periods of high
contention. Chains that very often have high activity from multiple owners can
set the number of multi-leader rounds to 0.

For more detail and examples on how to open and close chains, see the wallet
section on [chain management](wallets.md#opening-a-chain).
