# Using EVM Smart Contracts on Linera

Thanks to the experimental integration of
[`Revm`](https://bluealloy.github.io/revm/) in Linera, it is now possible to run
smart contracts compiled for the Ethereum Virtual Machine (EVM) within Linera
microchains.

The main purpose of this integration is to allow opensource smart contracts
originally written for Ethereum to be more easily migrated to the Linera
protocol and composed with existing Linera applications.

## Overview and code examples

We generally assume that EVM smart contracts are written in
[Solidity](https://soliditylang.org/) and compiled to EVM bytecode using the
Solidity compiler.

Transactions running on the EVM are still following the programming model of the
Linera protocol, notably they can only access the local state of the microchain
that executes them. Inside a microchain, contracts may call each other as usual,
possibly across virtual machines.

The main features of Linera, such as cross-chain messaging, are also supported
in the EVM. These functionalities are exposed through a Solidity library
[`Linera.sol`](https://github.com/linera-io/linera-protocol/tree/main/linera-execution/solidity/Linera.sol).

Frontends may interact with EVM contracts using custom EVM-like RPCs or using a
Rust/Wasm application as a proxy.

Code examples for the features described below can be found under the directory
[`linera-service/tests/fixtures`](https://github.com/linera-io/linera-protocol/tree/main/linera-service/tests/fixtures).

## Publishing EVM smart contracts

The process for publishing EVM smart contracts is similar to that for Wasm smart
contracts, with the key difference being the need to specify the virtual machine
used (the default is Wasm).

For EVM contracts, there is only one bytecode file (unlike Wasm, which requires
separate `contract` and `service` binaries). Therefore, the same file must be
passed twice:

```bash
linera publish-and-create \
  counter.bytecode counter.bytecode \
  --vm-runtime evm \
  --json-parameters "42"
```

Here, `counter.bytecode` contains the compiled contract, and "42" is passed as a
constructor argument via the `--json-parameters` flag.

Constructor arguments for the EVM contract are passed through application
parameters. Instantiation-specific arguments are provided separately.

## Calling other smart contracts.

EVM smart contracts on Linera can invoke other EVM contracts using standard
Solidity syntax.

### EVM Contracts calling Wasm smart contracts.

To call a Wasm smart contract from an EVM contract, use the following Solidity
command:

```solidity
	bytes memory return_value = Linera.try_call_application(address, input);
```

- `address`: the address of the Wasm smart contract, as a `bytes32`.
- `input`: the bytes representing the BCS-serialized `ContractAbi::Operation`
  input.

The serialization code can be generated using the `serde-reflection` crate.

### Wasm smart contracts calling EVM Contracts.

Wasm smart contracts can call EVM contracts using the `alloy-sol-types` crate.
This crate enables construction of Solidity-compatible types and supports RLP
serialization/deserialization.

The Wasm contract call-evm-counter demonstrates this functionality.

- For operations, the input type is `Vec<u8>`.

- For service calls, the input type is `EvmQuery`.

Note: Linera distinguishes between contract and service code execution contexts.

## Multichain EVM applications.

To operate across multiple chains, an EVM application must implement the
following functions:

```solidity
    function instantiate(bytes memory input) external
    function execute_message(bytes memory input) external
```

- `instantiate` is called on the creator chain.

- `execute_message` handles incoming cross-chain messages.

Additional SDK functions available include:

```solidity
    Linera.chain_ownership()
    Linera.read_data_blob()
    Linera.assert_data_blob_exists()
    Linera.validation_round()
    Linera.message_id()
    Linera.message_is_bouncing()
```

## Difference between EVM applications in Ethereum and Linera.

- `Reentrancy`: Reentrancy is currently not supported on Linera. Contracts
  relying on it will fail with a clean error. In the future, reentrancy may be
  allowed as an option.

- `Address Computation`: Contract addresses are computed differently from
  Ethereum.

- `Gas Limits`: Following Infura's practice, Linera imposes a gas limit of
  20,000,000 for service calls in EVM contracts. Contract execution is similarly
  constrained.
