# Mempool-Driven Chains

Mempool-driven chains implement a replicated state machine, similar to a sharded blockchain. They let any user publicly submit a command to advance the replicated state.

This feature is more powerful than [multi-owner chains](multi_owner.md), which allow a fixed sets of stakeholders to manage the same chain, assuming minimal collaboration between stakeholders.

However, it requires a full BFT protocol and a mempool (which could be the same thing in the case a DAG-based protocol such as [BullShark](https://arxiv.org/abs/2201.05677)).

## Requirements

* Make it possible for validators to communicate directly and synchronize a DAG => This should be easy by having each validator also run a "client".

* Choose and implement a DAG BFT protocol such as [BullShark](https://arxiv.org/abs/2201.05677)).

    - Alternatively, we could use the code for multi-owner chains, add VDF-based mining(?) as a basic leader election between validators, and re-use the existing infrastructure of user chains + cross-shard requests as a mempool.
