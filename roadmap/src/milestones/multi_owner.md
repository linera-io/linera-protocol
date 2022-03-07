# Multi-Owner Accounts

The goal of this milestone is to allow certain accounts to be configured so that multiple stakeholders can safely issue commands on the same account.

* Stakeholder may involuntarily post conflicting commands.

* Initially, we will assume a common purpose between stakeholders so that nobody wishes to actively block liveness of the account.

* Used together with a special type of accounts, this is the basis to support atomic-swaps.

* Even single-owner accounts may require this feature so that system agents may carry automated tasks (e.g. [reconfiguration](reconfiguration.md), [code migration](extensibility.md), [trade confirmation](order_book.md)).

## Requirements

* The authentication of commands is generalized to allow a minimal one-shot BFT protocol to take place. (See [draft](https://arxiv.org/abs/2201.05073) on atomic swaps)

* Optionally, a simple leader rotation and timeouts are implemented to enforce liveness in the usual BFT sense.
