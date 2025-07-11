# Delegated fungible Example Application

This example application implements a delegated fungible. It is the equivalent for Linera
of Ethereum ERC20 contract and Solana SPL. That is the difference with `fungible` is that
allowances are supported.

## How It Works

The API `approve` and `transfer_from` have been added to the functionality, following the ERC20
contract.

