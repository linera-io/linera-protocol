# Extensibility

This milestone aims to generalize the replicated state machine in each account, so that accounts can accept various sets of commands.

## Requirements

* At first, we may introduce a notion of *account program* and support only a fixed set of programs. For instance:

    1. Configuration management (aka "beacon chain")

    2. User account

    3. Atomic swaps

* Later, we may enrich the platform with a VM that makes it easy to extend a deployment with new programs. This may require a new type of account:

    3. Code manager (registers a program and manages its successive different versions)

* Switching an account to a new (major) version number requires an explicit command from the account owner.

    - TODO: Should program have minor version numbers that are auto-migrated?

* Initially, user accounts will support money transfers in the native currency only (see [FastPay](https://arxiv.org/abs/2003.11506)).

* Eventually, we should support user-defined assets and wrapped currencies from external blockchains. E.g. the [Wormhole bridge](https://wormholenetwork.com/) of Solana is used to bridge ERC-22 to SPL (see also [this note](https://medium.com/the4thpillar/ethereum-erc-20-four-to-solana-spl-four-token-bridge-manual-10c33e64030f))

* TODO: What kind of VM should we use?
