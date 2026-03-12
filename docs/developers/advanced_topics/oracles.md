# Oracles and Ethereum

> Oracles are a mechanism that allows applications to get information from the
> outside world that is not on-chain yet. One example is accessing the state of
> an Ethereum smart contract from a Linera application.

The contract runtime currently has three oracle methods:

- `query_service` allows the contract to make a call to the application's own
  service code. Services can access some off-chain information, so these are not
  guaranteed to return the same result each time they are called.
- `http_post` makes an HTTP POST request and returns the response.
- `assert_before` asserts that the block is being validated before a given time.

The first two are disabled on public devnets and testnets for now, but can be
used locally by compiling with the `unstable-oracles` flag.

Applications should use these methods only in ways that make it very likely that
all validators see the same result, otherwise any block proposals running that
application's code are likely to fail, hurting the liveness of the users'
chains. (Also, oracle methods are disallowed completely in fast rounds; see
[Chain Ownership Semantics](../core_concepts/microchains.md#chain-ownership-semantics).)

The Linera SDK uses `http_post` to implement the `EthereumClient` type, which
provides functions to query an Ethereum node:

- `get_balance` for accessing the balance of an Ethereum account at a specific
  block number.

- `read_events` for reading events from a specified Ethereum smart contract from
  one block to the last block.

- `non_executive_call` for executing a function in an Ethereum smart contract at
  a specific block, with the result not being included in the chain.

None of those functions are changing the state of the Ethereum blockchain. They
are all about observing the chain. All those functions take a specific block
number in input since otherwise different validators could read different number
of blocks.

The smart contract `ethereum-tracker` provides one example of such a contract
and the end-to-end test `test_wasm_end_to_end_ethereum_tracker` how the
interaction is actually done.
