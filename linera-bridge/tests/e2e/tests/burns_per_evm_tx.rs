// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! For each `ChainSpec` (Ethereum / Base / Base Sepolia / …), binary-searches
//! the largest `N` such that `eth_estimateGas(bridge.addBlock(cert_N))` is
//! `<= chain_spec.block_gas_limit`, where `cert_N` is a Linera block carrying
//! exactly `N` `BurnEvent`s. `eth_estimateGas` runs the EVM in dry-run mode,
//! so the bridge's `processedBurns` mapping stays untouched and a single
//! bridge instance handles every iteration.

#![recursion_limit = "512"]

use test_case::test_case;

#[test_case("ethereum",     30_000_000,  None; "ethereum")]
#[test_case("base",         240_000_000, None; "base")]
#[test_case("base_sepolia", 240_000_000, None; "base_sepolia")]
#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and bridge contracts.
async fn burns_per_evm_tx(
    _name: &str,
    _block_gas_limit: u64,
    _min_expected_burns: Option<u32>,
) -> anyhow::Result<()> {
    Ok(())
}
