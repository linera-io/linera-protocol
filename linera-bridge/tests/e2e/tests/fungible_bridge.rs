// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

//! End-to-end test: deploy a fungible token on Linera, transfer tokens to an EVM address,
//! submit the block certificate to FungibleBridge on Anvil, and verify the ERC20 balance.

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{Address, U256},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Context as _;
use futures::StreamExt as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Bytecode, U128},
    identifiers::AccountOwner,
    vm::VmRuntime,
};
use linera_bridge::abi::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, exec_ok, light_client_address,
    start_compose, wait_for_light_client, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{environment::wallet::Memory, worker::Reason};
use linera_execution::{Operation, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::DbStorage;
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use wrapped_fungible::{
    Account, InitialState, WrappedFungibleOperation, WrappedFungibleTokenAbi, WrappedParameters,
};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }
}

sol! {
    #[sol(rpc)]
    interface IFungibleBridge {
        function addBlock(bytes calldata data) external;
    }
}

#[tokio::test]
#[ignore] // Requires pre-built docker images and Wasm: `make -C linera-bridge build-all`
async fn test_fungible_bridge_transfers_to_evm() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-bridge-test";

    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    // ── 1. Create programmatic Linera client ──
    tracing::info!("Creating programmatic Linera client...");
    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;

    let config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &config,
        "bridge-e2e-test",
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

    // ── 2. Claim chain A from faucet ──
    tracing::info!("Claiming chain A from faucet...");
    let owner_a = AccountOwner::from(signer.generate_new());
    let chain_a_desc = faucet.claim(&owner_a).await?;
    let chain_a = chain_a_desc.id();
    ctx.extend_with_chain(chain_a_desc, Some(owner_a)).await?;

    let cc_a = ctx.make_chain_client(chain_a).await?;
    cc_a.synchronize_from_validators().await?;
    tracing::info!(%chain_a, %owner_a, "Chain A claimed");

    // Collect all chain A certificate bytes to submit to the bridge in sequential order.
    let mut chain_a_cert_bytes: Vec<Vec<u8>> = Vec::new();

    // ── 2b. Deploy LineraToken on Anvil ──
    // Done before creating the Linera apps so its address can be baked into both the
    // evm-bridge `token_address` and the wrapped-fungible `evm_token_address`.
    tracing::info!("Deploying LineraToken via forge script...");
    let erc20_addr = deploy_linera_token(&compose, project_name, &compose_file).await?;
    tracing::info!(%erc20_addr, "LineraToken deployed");

    // ── 3. Publish and create the wrapped-fungible app, then evm-bridge, on chain A ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let evm_bridge_wasm_dir =
        repo_root.join("linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release");
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    // 3a. Publish and create the wrapped-fungible app first. The evm-bridge app takes the
    // wrapped-fungible app id as a creation parameter, so it must exist beforehand.
    tracing::info!("Publishing wrapped-fungible module...");
    let contract_bytecode =
        Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let service_bytecode =
        Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;

    let (module_id, cert) = cc_a
        .publish_module(contract_bytecode, service_bytecode, VmRuntime::Wasm)
        .await?
        .expect("publish module committed");
    tracing::info!(height=?cert.inner().block().header.height, "Module published");
    chain_a_cert_bytes.push(bcs::to_bytes(&cert)?);

    cc_a.synchronize_from_validators().await?;
    let (inbox_certs, _) = cc_a.process_inbox().await?;
    for c in &inbox_certs {
        chain_a_cert_bytes.push(bcs::to_bytes(c)?);
    }

    tracing::info!("Creating wrapped-fungible application...");
    let params = WrappedParameters {
        ticker_symbol: "TEST".to_string(),
        decimals: 18,
        mint_chain_id: chain_a,
        evm_token_address: erc20_addr.0 .0,
        evm_source_chain_id: 31337,
    };
    let init_state = InitialState {
        accounts: BTreeMap::from([(owner_a, U128(1000u128 * 10u128.pow(18)))]),
    };
    let (app_id, create_cert): (
        linera_base::identifiers::ApplicationId<WrappedFungibleTokenAbi>,
        _,
    ) = cc_a
        .create_application(
            module_id.with_abi::<WrappedFungibleTokenAbi, _, _>(),
            &params,
            &init_state,
            vec![],
        )
        .await?
        .expect("create application committed");
    let app_id = app_id.forget_abi();
    tracing::info!(%app_id, "Application created");
    chain_a_cert_bytes.push(bcs::to_bytes(&create_cert)?);

    // 3b. Publish and create the evm-bridge app, pointing it at the wrapped-fungible app
    // via `fungible_app_id`. It drives the burn: a user submits `BridgeOperation::Burn`
    // here, and the app emits the `BurnEvent` that FungibleBridge matches against its
    // `bridgeApplicationId`.
    tracing::info!("Publishing evm-bridge module...");
    let eb_contract =
        Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_contract.wasm"))?;
    let eb_service = Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_service.wasm"))?;
    let (eb_module_id, eb_publish_cert) = cc_a
        .publish_module(eb_contract, eb_service, VmRuntime::Wasm)
        .await?
        .expect("publish evm-bridge module committed");
    tracing::info!(height=?eb_publish_cert.inner().block().header.height, "evm-bridge module published");
    chain_a_cert_bytes.push(bcs::to_bytes(&eb_publish_cert)?);
    cc_a.synchronize_from_validators().await?;
    let (eb_inbox_certs, _) = cc_a.process_inbox().await?;
    for c in &eb_inbox_certs {
        chain_a_cert_bytes.push(bcs::to_bytes(c)?);
    }

    tracing::info!("Creating evm-bridge application...");
    let (bridge_app_id, eb_create_cert) = cc_a
        .create_application_untyped(
            eb_module_id,
            serde_json::to_vec(&BridgeParameters {
                source_chain_id: 31337,
                token_address: erc20_addr.0 .0,
                bridge_chain_id: chain_a,
                fungible_app_id: app_id,
            })?,
            serde_json::to_vec(&BridgeInstantiationArgument {
                rpc_endpoint: String::new(),
            })?,
            vec![],
        )
        .await?
        .expect("create evm-bridge app committed");
    tracing::info!(%bridge_app_id, "evm-bridge app created");
    chain_a_cert_bytes.push(bcs::to_bytes(&eb_create_cert)?);

    // 3c. Register the evm-bridge app on the wrapped-fungible app so the bridge can
    // drive the burn. This must happen before any Burn operation is submitted.
    tracing::info!("Registering evm-bridge app in wrapped-fungible...");
    let register_bytes = bcs::to_bytes(&WrappedFungibleOperation::RegisterAuthorizedCaller {
        app_id: bridge_app_id,
    })?;
    let register_op = Operation::User {
        application_id: app_id,
        bytes: register_bytes,
    };
    let register_cert = cc_a
        .execute_operations(vec![register_op], vec![])
        .await?
        .expect("register bridge app committed");
    chain_a_cert_bytes.push(bcs::to_bytes(&register_cert)?);

    // ── 4. Claim chain B (user chain) from faucet and subscribe to notifications ──
    tracing::info!("Claiming chain B from faucet...");
    let owner_b = AccountOwner::from(signer.generate_new());
    let chain_b_desc = faucet.claim(&owner_b).await?;
    let chain_b = chain_b_desc.id();
    ctx.extend_with_chain(chain_b_desc, Some(owner_b)).await?;
    tracing::info!(%chain_b, "Chain B claimed");

    let cc_b = ctx.make_chain_client(chain_b).await?;
    let mut notifications = cc_b.subscribe()?;
    let (listener, _abort_handle, _) = cc_b.listen().await?;
    tokio::spawn(listener);

    // ── 6. Deploy FungibleBridge on Anvil ──
    // The wrapped-fungible app id (`app_id_bytes32`) is still required as the deposit
    // target; the new `bridge_app_id_bytes32` is what the bridge matches BurnEvents
    // against (burns are now driven and emitted by the evm-bridge app).
    let app_id_bytes32 = format!("0x{}", app_id.application_description_hash);
    let bridge_app_id_bytes32 = format!("0x{}", bridge_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");

    tracing::info!("Deploying FungibleBridge via forge script...");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &app_id_bytes32,
        &bridge_app_id_bytes32,
    )
    .await?;
    tracing::info!(%bridge_addr, "FungibleBridge deployed");

    // ── 7. Fund FungibleBridge with ERC20 tokens ──
    tracing::info!("Funding FungibleBridge with ERC20 tokens...");
    exec_ok(
        &compose,
        "foundry-tools",
        &format!(
            "cast send --rpc-url http://anvil:8545 \
             --private-key {ANVIL_PRIVATE_KEY} \
             {erc20_addr} \
             'transfer(address,uint256)(bool)' \
             {bridge_addr} \
             500000000000000000000"
        ),
        project_name,
        &compose_file,
    )
    .await;

    // ── 8. Transfer tokens from chain A to owner_b on chain B ──
    let evm_recipient = "70997970C51812dc3A010C7d01b50e0d17dc79C8";
    let receiver: AccountOwner = format!("0x{evm_recipient}").parse()?;

    tracing::info!("Sending wrapped-fungible transfer to owner_b on chain B...");
    let transfer_bytes = bcs::to_bytes(&WrappedFungibleOperation::Transfer {
        owner: owner_a,
        amount: U128(100u128 * 10u128.pow(18)),
        target_account: Account {
            chain_id: chain_b,
            owner: owner_b,
        },
    })?;
    let transfer_op = Operation::User {
        application_id: app_id,
        bytes: transfer_bytes,
    };
    let transfer_cert = cc_a
        .execute_operations(vec![transfer_op], vec![])
        .await?
        .expect("transfer committed");
    let transfer_block = transfer_cert.inner().block();
    tracing::info!(
        height=?transfer_block.header.height,
        messages=transfer_block.body.messages.len(),
        recipients=?transfer_block.recipients(),
        "Transfer block submitted"
    );
    chain_a_cert_bytes.push(bcs::to_bytes(&transfer_cert)?);

    // ── 9. Wait for incoming bundle notification, then process inbox on chain B ──
    tracing::info!("Waiting for NewIncomingBundle notification on chain B...");
    tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(notification) = notifications.next().await {
            if matches!(notification.reason, Reason::NewIncomingBundle { .. }) {
                tracing::info!("Received NewIncomingBundle notification");
                return;
            }
        }
        panic!("Notification stream ended without NewIncomingBundle");
    })
    .await?;

    tracing::info!("Processing inbox on chain B...");
    cc_b.synchronize_from_validators().await?;
    let (certs, _) = cc_b.process_inbox().await?;
    tracing::info!(count = certs.len(), "Processed certificates from inbox");
    assert!(
        !certs.is_empty(),
        "process_inbox should produce at least one certificate"
    );

    // ── 10. Burn: owner_b submits a `BridgeOperation::Burn` to the evm-bridge app on
    // chain B. The bridge moves the tokens into its escrow on the bridge chain
    // (chain A) and burns them, emitting the `BurnEvent` there. ──
    let evm_target: [u8; 20] = match receiver {
        AccountOwner::Address20(bytes) => bytes,
        other => anyhow::bail!("expected Address20 receiver, got {other:?}"),
    };

    tracing::info!("Submitting Burn operation to evm-bridge app on chain B...");
    let burn_bytes = bcs::to_bytes(&BridgeOperation::Burn {
        amount: U128(100u128 * 10u128.pow(18)),
        evm_target,
    })?;
    let burn_op = Operation::User {
        application_id: bridge_app_id,
        bytes: burn_bytes,
    };
    cc_b.execute_operations(vec![burn_op], vec![])
        .await?
        .expect("burn operation committed");

    // ── 11. Process inbox on chain A so the Credit+Burn bundle lands and the
    // evm-bridge app emits the `BurnEvent` in a chain A block. Collect those certs
    // so the BurnEvent block is submitted to FungibleBridge. ──
    tracing::info!("Processing inbox on chain A to drive the burn...");
    cc_a.synchronize_from_validators().await?;
    let (inbox_a_certs, _) = cc_a.process_inbox().await?;
    for c in &inbox_a_certs {
        chain_a_cert_bytes.push(bcs::to_bytes(c)?);
    }

    // ── 12. Submit all chain A blocks to FungibleBridge sequentially ──
    // Step 11's inbox processing landed the Credit+Burn bundle, so the evm-bridge app
    // burned the escrowed tokens and emitted a `BurnEvent` in a chain A block (collected
    // above). FungibleBridge matches that event against its `bridgeApplicationId`.
    tracing::info!(
        count = chain_a_cert_bytes.len(),
        "Submitting all chain A blocks to bridge"
    );

    let rpc_url = "http://localhost:8545".parse()?;
    let evm_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let evm_wallet = EthereumWallet::from(evm_signer);
    let provider = ProviderBuilder::new()
        .wallet(evm_wallet)
        .connect_http(rpc_url);

    let bridge_contract = IFungibleBridge::new(bridge_addr, &provider);
    for (i, cert_bytes) in chain_a_cert_bytes.iter().enumerate() {
        let tx = bridge_contract
            .addBlock(cert_bytes.clone().into())
            .send()
            .await?;
        let receipt = tx.get_receipt().await?;
        tracing::info!(i, tx=?receipt.transaction_hash, "addBlock transaction submitted");
    }

    // ── 14. Verify ERC20 balance ──
    let evm_recipient_addr: Address = format!("0x{evm_recipient}").parse()?;
    let erc20_contract = IERC20::new(erc20_addr, &provider);
    let balance = erc20_contract.balanceOf(evm_recipient_addr).call().await?;
    tracing::info!(?balance, "ERC20 balance of recipient");

    // 100 tokens = 100 * 10^18 (Amount uses 18 decimal places)
    let expected_balance = U256::from(100u128 * 10u128.pow(18));
    assert_eq!(
        balance, expected_balance,
        "ERC20 balance should match the transferred amount"
    );

    tracing::info!("Test passed! ERC20 balance matches transferred amount.");
    Ok(())
}
