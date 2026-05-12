// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! For each `ChainSpec` (Ethereum / Base / Base Sepolia / …), binary-searches
//! the largest `N` such that `eth_estimateGas(bridge.addBlock(cert_N))` is
//! `<= chain_spec.block_gas_limit`, where `cert_N` is a Linera block carrying
//! exactly `N` `BurnEvent`s. `eth_estimateGas` runs the EVM in dry-run mode,
//! so the bridge's `processedBurns` mapping stays untouched and a single
//! bridge instance handles every iteration.

#![recursion_limit = "512"]

use alloy::{providers::ProviderBuilder, sol};
use linera_base::{crypto::InMemorySigner, identifiers::AccountOwner};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, fund_bridge_erc20,
    light_client_address, publish_and_create_wrapped_fungible, set_anvil_block_gas_limit,
    start_compose, wait_for_light_client,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::WasmRuntime;
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use test_case::test_case;

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

/// Hard cap on the search range. Bounds chain-A block construction cost and
/// keeps the test runtime predictable even for very-high-gas chains.
const MAX_SEARCH_N: u32 = 2048;

/// Chain-B pre-funded balance: enough wrapped-fungible to issue
/// `MAX_SEARCH_N * doubling_plus_bisect_iterations` `Transfer` ops. 22 search
/// iterations × 2048 burns × 1 token each = 45 056 tokens upper bound; round
/// up to 100 000 for headroom.
const INITIAL_BALANCE_TOKENS: u128 = 100_000;

/// Each burn moves exactly one wrapped-fungible token to a fresh
/// `Address20`. Constant token amount keeps per-burn EVM gas constant
/// (the per-burn cost is dominated by the storage write + ERC-20 transfer,
/// not by the amount value).
const BURN_AMOUNT_TOKENS: u128 = 1;

#[test_case("ethereum",     30_000_000,  None; "ethereum")]
#[test_case("base",         240_000_000, None; "base")]
#[test_case("base_sepolia", 240_000_000, None; "base_sepolia")]
#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and bridge contracts.
async fn burns_per_evm_tx(
    name: &str,
    block_gas_limit: u64,
    min_expected_burns: Option<u32>,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();

    let compose_file = compose_file_path();
    let project_name = format!("linera-burns-per-tx-{name}");

    let compose = start_compose(&compose_file, &project_name).await;
    wait_for_light_client(&compose, &project_name, &compose_file).await;

    let rpc_url = "http://localhost:8545".parse()?;
    let provider = ProviderBuilder::new().connect_http(rpc_url);

    // Raise anvil's own block gas ceiling well above the chain we are
    // measuring so `eth_estimateGas` never errors out on anvil's limit.
    set_anvil_block_gas_limit(&provider, block_gas_limit.saturating_mul(5)).await?;

    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;

    let store_config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &store_config,
        &format!("burns-per-tx-{name}-e2e"),
        Some(WasmRuntime::default()),
        StorageCacheConfig {
            blob_cache_size: 1000,
            confirmed_block_cache_size: 1000,
            certificate_cache_size: 1000,
            certificate_raw_cache_size: 1000,
            event_cache_size: 1000,
            cache_cleanup_interval_secs: linera_storage::DEFAULT_CLEANUP_INTERVAL_SECS,
        },
    )
    .await?;
    genesis_config.initialize_storage(&mut storage).await?;

    let mut signer = InMemorySigner::new(None);
    let mut ctx = ClientContext::new(
        storage,
        Memory::default(),
        signer.clone(),
        &Default::default(),
        None,
        genesis_config,
        linera_core::worker::DEFAULT_BLOCK_CACHE_SIZE,
        linera_core::worker::DEFAULT_EXECUTION_STATE_CACHE_SIZE,
    )
    .await?;

    let owner_a = AccountOwner::from(signer.generate_new());
    let chain_a_desc = faucet.claim(&owner_a).await?;
    let chain_a = chain_a_desc.id();
    ctx.extend_with_chain(chain_a_desc, Some(owner_a)).await?;
    let cc_a = ctx.make_chain_client(chain_a).await?;
    cc_a.synchronize_from_validators().await?;

    let owner_b = AccountOwner::from(signer.generate_new());
    let chain_b_desc = faucet.claim(&owner_b).await?;
    let chain_b = chain_b_desc.id();
    ctx.extend_with_chain(chain_b_desc, Some(owner_b)).await?;
    let cc_b = ctx.make_chain_client(chain_b).await?;
    cc_b.synchronize_from_validators().await?;

    let erc20_addr = deploy_linera_token(&compose, &project_name, &compose_file).await?;
    let fungible_app_id = publish_and_create_wrapped_fungible(
        &cc_b,
        owner_b,
        chain_a,
        erc20_addr,
        INITIAL_BALANCE_TOKENS,
    )
    .await?;

    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        &project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &app_id_bytes32,
    )
    .await?;

    // Fund the bridge with enough ERC-20 to cover up to MAX_SEARCH_N
    // releases even though estimation is a dry run — keeps the test
    // robust if we later switch from estimate to live submission.
    fund_bridge_erc20(
        &compose,
        &project_name,
        &compose_file,
        erc20_addr,
        bridge_addr,
        u128::from(MAX_SEARCH_N) * BURN_AMOUNT_TOKENS * 10u128.pow(18),
    )
    .await;

    // Suppress unused-variable warnings; subsequent task fills these in.
    let _ = (
        &cc_a,
        &cc_b,
        owner_b,
        chain_a,
        fungible_app_id,
        bridge_addr,
        block_gas_limit,
        min_expected_burns,
        &provider,
    );

    Ok(())
}
