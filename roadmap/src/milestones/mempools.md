# Mempool-Driven Accounts

Mempool-driven accounts implement a replicated state machine, similar to a sharded blockchain. They let any user publicly submit a command to advance the replicated state.

This feature is more powerful than [multi-owner accounts](multi_owner.md), which allow a fixed sets of stakeholders to manage the same account, assuming minimal collaboration between stakeholders.

However, it requires a full BFT protocol and a mempool (which could be the same thing in the case a DAG-based protocol such as [BullShark](https://arxiv.org/abs/2201.05677)).

## Requirements

* Make it possible for validators to communicate directly and synchronize a DAG

* Choose and implement a DAG BFT protocol such as [BullShark](https://arxiv.org/abs/2201.05677)).
