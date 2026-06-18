// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! For each `ChainSpec` (Ethereum / Base / Base Sepolia / …), binary-searches
//! the largest `N` such that settling a Linera block carrying exactly `N`
//! `BurnEvent`s costs `<= chain_spec.block_gas_limit` in EVM gas. Each block is
//! settled the way the relayer settles it: `registerBlock` once, then a
//! `processBurns` call per transaction; the reported gas is the sum of the
//! per-transaction `eth_estimateGas(processBurns)` estimates. `registerBlock`
//! mutates chain state (one registered block per iteration), but the
//! `processBurns` estimates are dry-run, so the bridge's `processedBurns`
//! mapping stays untouched and a single bridge instance handles every iteration.

#![recursion_limit = "512"]

use alloy::{network::EthereumWallet, providers::ProviderBuilder, signers::local::PrivateKeySigner};
use linera_base::{crypto::InMemorySigner, data_types::U128, identifiers::AccountOwner};
use linera_bridge::{abi::BridgeOperation, relay::evm::EvmClient};
use linera_bridge_e2e::{
    burn_positions_by_tx, compose_file_path, deploy_fungible_bridge,
    deploy_linera_token_with_supply, fetch_latest_cert, fund_bridge_erc20, light_client_address,
    publish_and_create_evm_bridge, publish_and_create_wrapped_fungible, register_bridge_app,
    set_anvil_block_gas_limit, start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use test_case::test_case;

/// Hard cap on the search range. Bounds chain-A block construction cost
/// and keeps the test runtime predictable. The ERC-20 supply minted to
/// the bridge is sized off this constant (see `deploy_linera_token_with_supply`
/// call below), so widening it just costs more iterations — no balance
/// constraint.
const MAX_SEARCH_N: u32 = 1000;

/// Chain-B pre-funded balance in raw ERC-20 sub-units. Worst-case burn
/// total is `MAX_SEARCH_N * ~10 iterations * BURN_AMOUNT_TOKENS * 10^18`
/// ≈ 10^37 raw; 10^38 gives 10× headroom and still fits in `u128`.
const INITIAL_BALANCE_TOKENS: u128 = 10u128.pow(38);

/// Each burn moves exactly this many wrapped-fungible tokens to a fresh
/// `Address20`. Constant token amount keeps per-burn EVM gas constant
/// (the per-burn cost is dominated by the storage write + ERC-20 transfer,
/// not by the amount value).
const BURN_AMOUNT_TOKENS: u128 = 10u128.pow(15);

// Floors are a sanity lower bound, not a tight target. The previous numbers were
// measured under the whole-block `addBlock` path; the reported gas is now the summed
// `processBurns` estimate across the block's transactions, a different figure.
// Re-measure and bump these floors once `processBurns` numbers are taken under this flow.
#[test_case("ethereum",     30_000_000,  Some(30); "ethereum")]
#[test_case("base",         240_000_000, Some(140); "base")]
#[tokio::test]
#[serial_test::serial]
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
    // A wallet-backed provider is required because settlement now `registerBlock`s each
    // block (a signed, state-changing tx) before estimating its `processBurns` gas.
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

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
        linera_bridge_e2e::test_storage_cache_config(),
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

    // The evm-bridge app drives the burn; create it on the bridge/mint chain
    // (chain A) after the wrapped-fungible app so its id can be baked in.
    let bridge_app_id =
        publish_and_create_evm_bridge(&cc_a, erc20_addr, chain_a, fungible_app_id).await?;

    // Register the wrapped-fungible app with the evm-bridge so it can drive
    // the escrow transfer + burn.
    register_bridge_app(&cc_a, fungible_app_id, bridge_app_id).await?;

    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let bridge_app_id_bytes32 = format!("0x{}", bridge_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        &project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &app_id_bytes32,
        &bridge_app_id_bytes32,
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

    // `n` is past the upper bound when its processBurns estimate exceeds the EVM block gas limit,
    // or when `n` burns do not fit in a single Linera block (`None`).
    let over_limit = |gas: Option<u64>| !matches!(gas, Some(g) if g <= block_gas_limit);

    let mut hi: u32 = 8;
    let mut hi_gas = build_and_estimate(
        hi,
        &cc_a,
        &cc_b,
        bridge_app_id,
        bridge_addr,
        &provider,
    )
    .await?;
    tracing::info!(n = hi, gas = ?hi_gas, "search: initial `Burn` ops count");

    while !over_limit(hi_gas) && hi < MAX_SEARCH_N {
        let next_hi = hi.saturating_mul(2).min(MAX_SEARCH_N);
        let gas = build_and_estimate(
            next_hi,
            &cc_a,
            &cc_b,
            bridge_app_id,
            bridge_addr,
            &provider,
        )
        .await?;
        hi = next_hi;
        hi_gas = gas;
        if over_limit(hi_gas) {
            tracing::info!(burn_ops = next_hi, ?gas, "search: found upper bound");
            break;
        }
        tracing::info!(
            burn_ops = next_hi,
            ?gas,
            "search: doubling, still under limit"
        );
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
            bridge_app_id,
            bridge_addr,
            &provider,
        )
        .await?
    }
    .expect("n=1 must fit in a single Linera block");
    anyhow::ensure!(
        lo_gas <= block_gas_limit,
        "n=1 already exceeds block_gas_limit ({lo_gas} > {block_gas_limit}); test cannot proceed"
    );

    // If even the doubling cap fits under both limits, we cannot bound from above — report the cap.
    if !over_limit(hi_gas) {
        tracing::warn!(
            cap = MAX_SEARCH_N,
            gas = ?hi_gas,
            block_gas_limit,
            "search hit MAX_SEARCH_N without exceeding gas limit; reported max_n is the cap"
        );
        let max_n = hi;
        tracing::info!(
            chain = name,
            max_n,
            gas_at_max_n = ?hi_gas,
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
        let gas = build_and_estimate(
            mid,
            &cc_a,
            &cc_b,
            bridge_app_id,
            bridge_addr,
            &provider,
        )
        .await?;
        tracing::info!(n = mid, ?gas, "search: bisect");
        if over_limit(gas) {
            hi = mid;
            hi_gas = gas
        } else {
            lo = mid;
            lo_gas = gas.expect("under limit => fits in one block");
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

/// Bundles `n` `BridgeOperation::Burn` ops into a single chain-B block;
/// each routes a funding transfer + tracked `BridgeMessage::Burn` to the
/// bridge chain (chain A). Drives `cc_a.process_inbox()` (which produces
/// one chain-A block with `n` `BurnEvent`s), reads the resulting
/// `ConfirmedBlockCertificate`, `registerBlock`s it, and sums the
/// `eth_estimateGas(processBurns)` estimate of each of its transactions —
/// the EVM gas to settle the whole block the way the relayer does.
///
/// Returns `Some(gas)` for the estimate, or `None` when `n` burns do not fit in a single
/// Linera block (chain A split them across blocks) — an upper bound on `max_n` that the
/// caller treats like exceeding `block_gas_limit`. Reverts surface as `Err`.
async fn build_and_estimate<P, E>(
    n: u32,
    cc_a: &linera_core::client::ChainClient<E>,
    cc_b: &linera_core::client::ChainClient<E>,
    bridge_app_id: linera_base::identifiers::ApplicationId,
    bridge_addr: alloy::primitives::Address,
    provider: &P,
) -> anyhow::Result<Option<u64>>
where
    P: alloy::providers::Provider + Clone,
    E: linera_core::environment::Environment,
{
    use anyhow::Context as _;

    let burn_amount = U128(BURN_AMOUNT_TOKENS * 10u128.pow(18));
    let operations = (1..=n)
        .map(|i| {
            // Deterministic distinct recipients; high bytes spell out the
            // iteration counter so log inspection makes the address ↔ burn
            // mapping easy to read. Start from 1 — `Address20(0…0)` is the
            // zero address and ERC-20 rejects transfers to it.
            let mut evm_target = [0u8; 20];
            evm_target[16..].copy_from_slice(&i.to_be_bytes());
            let burn_bytes = bcs::to_bytes(&BridgeOperation::Burn {
                amount: burn_amount,
                evm_target,
            })
            .expect("BCS serialization");
            Operation::User {
                application_id: bridge_app_id,
                bytes: burn_bytes,
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
    // All `n` burns must land in one chain-A block to be settled together. If chain A split
    // them across blocks, `n` exceeds a single block's capacity — an upper bound on `max_n`,
    // not an error.
    if height_after.0 != height_before.0 + 1 {
        tracing::info!(
            n,
            before = height_before.0,
            after = height_after.0,
            "search: n exceeds one Linera block"
        );
        return Ok(None);
    }

    let cert = fetch_latest_cert(cc_a).await?;
    let by_tx = burn_positions_by_tx(&cert, bridge_app_id);
    anyhow::ensure!(!by_tx.is_empty(), "chain-A block carried no BurnEvents for n={n}");

    // Settle the block the way the relayer does: register it once, then estimate the
    // `processBurns` gas of each of its transactions. The reported figure is the sum
    // across the block's transactions.
    let evm_client = EvmClient::new(
        provider.clone(),
        bridge_addr,
        alloy::primitives::Address::ZERO,
        Some(light_client_address()),
    );
    evm_client.register_block(&cert).await?;

    let mut total_gas: u64 = 0;
    for (tx_index, positions) in by_tx {
        let gas = evm_client
            .estimate_process_burns_gas(&cert, tx_index, &positions)
            .await
            .with_context(|| format!("estimate_gas(processBurns) for n={n}, tx {tx_index}"))?;
        total_gas = total_gas.saturating_add(gas);
    }

    Ok(Some(total_gas))
}
