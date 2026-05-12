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
use linera_base::{crypto::InMemorySigner, data_types::Amount, identifiers::AccountOwner};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token_with_supply, fetch_latest_cert,
    fund_bridge_erc20, light_client_address, publish_and_create_wrapped_fungible,
    set_anvil_block_gas_limit, start_compose, wait_for_light_client,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use test_case::test_case;
use wrapped_fungible::{Account, WrappedFungibleOperation};

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

/// Hard cap on the search range. Bounds chain-A block construction cost
/// and keeps the test runtime predictable. The ERC-20 supply minted to
/// the bridge is sized off this constant (see `deploy_linera_token_with_supply`
/// call below), so widening it just costs more iterations — no balance
/// constraint.
const MAX_SEARCH_N: u32 = 1000;

/// Chain-B pre-funded balance: enough wrapped-fungible to issue
/// `MAX_SEARCH_N * doubling_plus_bisect_iterations * BURN_AMOUNT_TOKENS`
/// `Transfer` ops. Sized well above the worst case so the test never
/// runs out of source-chain balance regardless of `BURN_AMOUNT_TOKENS`.
const INITIAL_BALANCE_TOKENS: u128 = 10u128.pow(32);

/// Each burn moves exactly this many wrapped-fungible tokens to a fresh
/// `Address20`. Constant token amount keeps per-burn EVM gas constant
/// (the per-burn cost is dominated by the storage write + ERC-20 transfer,
/// not by the amount value).
const BURN_AMOUNT_TOKENS: u128 = 10u128.pow(15);

#[test_case("ethereum",     30_000_000,  Some(50); "ethereum")]
#[test_case("base",         240_000_000, Some(190); "base")]
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

    // Mint enough so the bridge can be funded for the largest search step.
    let token_supply_attos = u128::from(MAX_SEARCH_N) * BURN_AMOUNT_TOKENS * 10u128.pow(18);
    let erc20_addr =
        deploy_linera_token_with_supply(&compose, &project_name, &compose_file, token_supply_attos)
            .await?;
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

    // Fund the bridge with the full mint so a dry-run `token.transfer`
    // at the upper search bound never reverts for insufficient balance.
    fund_bridge_erc20(
        &compose,
        &project_name,
        &compose_file,
        erc20_addr,
        bridge_addr,
        token_supply_attos,
    )
    .await;

    let mut hi: u32 = 8;
    let mut hi_gas = build_and_estimate(
        hi,
        &cc_a,
        &cc_b,
        owner_b,
        chain_a,
        fungible_app_id,
        bridge_addr,
        &provider,
    )
    .await?;
    tracing::info!(n = hi, gas = hi_gas, "search: initial `Burn` ops count");

    while hi_gas <= block_gas_limit && hi < MAX_SEARCH_N {
        let next_hi = hi.saturating_mul(2).min(MAX_SEARCH_N);
        let gas = build_and_estimate(
            next_hi,
            &cc_a,
            &cc_b,
            owner_b,
            chain_a,
            fungible_app_id,
            bridge_addr,
            &provider,
        )
        .await?;
        hi = next_hi;
        hi_gas = gas;
        if hi_gas > block_gas_limit {
            tracing::info!(burn_ops = next_hi, gas, "search: found upper bound");
            break;
        }
        tracing::info!(burn_ops = next_hi, gas, "search: doubling, still under limit");
        if hi == MAX_SEARCH_N {
            break;
        }
    }

    let mut lo: u32 = 1;
    let mut lo_gas = if hi == 1 {
        hi_gas
    } else {
        build_and_estimate(
            lo,
            &cc_a,
            &cc_b,
            owner_b,
            chain_a,
            fungible_app_id,
            bridge_addr,
            &provider,
        )
        .await?
    };
    anyhow::ensure!(
        lo_gas <= block_gas_limit,
        "n=1 already exceeds block_gas_limit ({lo_gas} > {block_gas_limit}); test cannot proceed"
    );

    // If even the doubling cap fits, we cannot bound from above — report the cap.
    if hi_gas <= block_gas_limit {
        tracing::warn!(
            cap = MAX_SEARCH_N,
            gas = hi_gas,
            block_gas_limit,
            "search hit MAX_SEARCH_N without exceeding gas limit; reported max_n is the cap"
        );
        let max_n = hi;
        tracing::info!(
            chain = name,
            max_n,
            gas_at_max_n = hi_gas,
            block_gas_limit,
            "RESULT"
        );
        if let Some(floor) = min_expected_burns {
            anyhow::ensure!(
                max_n >= floor,
                "{name}: max_n={max_n} < min_expected_burns={floor}"
            );
        }
        return Ok(());
    }

    while lo + 1 < hi {
        let mid = lo + (hi - lo) / 2;
        let g = build_and_estimate(
            mid,
            &cc_a,
            &cc_b,
            owner_b,
            chain_a,
            fungible_app_id,
            bridge_addr,
            &provider,
        )
        .await?;
        tracing::info!(n = mid, gas = g, "search: bisect");
        if g <= block_gas_limit {
            lo = mid;
            lo_gas = g;
        } else {
            hi = mid;
            hi_gas = g;
        }
    }

    let _ = hi_gas; // silence dead_store warning in some toolchain configs
    let max_n = lo;
    let gas_at_max_n = lo_gas;
    tracing::info!(chain = name, max_n, gas_at_max_n, block_gas_limit, "RESULT");

    if let Some(floor) = min_expected_burns {
        anyhow::ensure!(
            max_n >= floor,
            "{name}: max_n={max_n} < min_expected_burns={floor}"
        );
    }

    Ok(())
}

/// Bundles `n` `WrappedFungibleOperation::Transfer` ops into a single
/// chain-B block, drives `cc_a.process_inbox()` (which produces one
/// chain-A block with `n` `BurnEvent`s), reads the resulting
/// `ConfirmedBlockCertificate`, BCS-encodes it, and asks anvil to
/// estimate the gas required by `bridge.addBlock(cert_bytes)`.
///
/// Returns the estimated gas. Reverts surface as `Err`.
#[allow(clippy::too_many_arguments)]
async fn build_and_estimate<P, E>(
    n: u32,
    cc_a: &linera_core::client::ChainClient<E>,
    cc_b: &linera_core::client::ChainClient<E>,
    owner_b: AccountOwner,
    chain_a: linera_base::identifiers::ChainId,
    fungible_app_id: linera_base::identifiers::ApplicationId,
    bridge_addr: alloy::primitives::Address,
    provider: &P,
) -> anyhow::Result<u64>
where
    P: alloy::providers::Provider,
    E: linera_core::environment::Environment,
{
    use anyhow::Context as _;

    let burn_amount = Amount::from_tokens(BURN_AMOUNT_TOKENS);
    let operations = (1..=n)
        .map(|i| {
            // Deterministic distinct recipients; high bytes spell out the
            // iteration counter so log inspection makes the address ↔ burn
            // mapping easy to read. Start from 1 — `Address20(0…0)` is the
            // zero address and ERC-20 rejects transfers to it.
            let mut bytes = [0u8; 20];
            bytes[16..].copy_from_slice(&i.to_be_bytes());
            let owner = AccountOwner::Address20(bytes);
            let withdraw_bytes = bcs::to_bytes(&WrappedFungibleOperation::Transfer {
                owner: owner_b,
                amount: burn_amount,
                target_account: Account {
                    chain_id: chain_a,
                    owner,
                },
            })
            .expect("BCS serialization");
            Operation::User {
                application_id: fungible_app_id,
                bytes: withdraw_bytes,
            }
        })
        .collect::<Vec<_>>();

    cc_b.synchronize_from_validators().await?;
    cc_b.execute_operations(operations, vec![])
        .await?
        .expect("chain-B execute_operations committed");

    cc_a.synchronize_from_validators().await?;
    let height_before = cc_a.chain_info().await?.next_block_height;
    cc_a.process_inbox().await?;
    let height_after = cc_a.chain_info().await?.next_block_height;
    anyhow::ensure!(
        height_after.0 == height_before.0 + 1,
        "chain A must produce exactly ONE block (n={n}, before={height_before}, after={height_after})"
    );

    let cert = fetch_latest_cert(cc_a).await?;
    let cert_bytes = bcs::to_bytes(&cert).context("BCS-serialize cert")?;

    let bridge = IFungibleBridge::new(bridge_addr, provider);
    let gas = bridge
        .addBlock(cert_bytes.into())
        .estimate_gas()
        .await
        .with_context(|| format!("estimate_gas(addBlock) for n={n}"))?;

    Ok(gas)
}
