// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end coverage of the blocked-burn refund flow.
//!
//! Chain B burns wrapped tokens targeting an EVM recipient. Before the
//! relayer's normal `addBlock` lands, the test driver calls
//! `FungibleBridge.blockBurn(cert, txIndex, eventPosInTx)` directly,
//! which emits `BurnBlocked` and flips `isBurnBlocked` to true on the
//! bridge contract. The relayer's EVM scanner picks up that log, builds
//! a refund proof, and submits `RefundBurn` on the evm-bridge Linera
//! app, which calls back into the wrapped-fungible app to mint the
//! burned amount back to the original burner on chain B.
//!
//! Final state:
//! - Chain B owner_b balance is back at the pre-burn level.
//! - EVM recipient ERC-20 balance stayed at 0 (no `_onBlock` release).
//! - `isBurnProcessed(height, eventIndex)` is `false`.
//! - `isBurnBlocked(height, eventIndex)` is `true`.
//! - `linera_bridge_refunds_completed` reached 1.

#![recursion_limit = "512"]

use std::{collections::BTreeMap, path::PathBuf, time::Duration};

use alloy::{
    network::EthereumWallet,
    primitives::{Address, U256},
    providers::ProviderBuilder,
    signers::local::PrivateKeySigner,
    sol,
};
use anyhow::Context as _;
use linera_base::{
    crypto::InMemorySigner,
    data_types::{Bytecode, Event, U128},
    identifiers::{AccountOwner, ApplicationId, GenericApplicationId},
    vm::VmRuntime,
};
use linera_bridge::{
    abi::{BridgeInstantiationArgument, BridgeOperation, BridgeParameters},
    proof::{parse_burn_blocked_event, ReceiptLog, RefundKey},
};
use linera_bridge_e2e::{
    compose_file_path, deploy_fungible_bridge, deploy_linera_token, fetch_latest_cert,
    fund_bridge_erc20, light_client_address, parse_metric_value, start_compose,
    wait_for_light_client, wait_for_relay_http_ready, ANVIL_PRIVATE_KEY,
};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::environment::wallet::Memory;
use linera_execution::{Operation, Query, QueryResponse, WasmRuntime};
use linera_faucet_client::Faucet;
use linera_storage::{DbStorage, StorageCacheConfig};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use wrapped_fungible::{
    Account, BurnEvent, InitialState, WrappedFungibleOperation, WrappedParameters,
};

sol! {
    #[sol(rpc)]
    interface IERC20 {
        function balanceOf(address account) external view returns (uint256);
    }

    #[sol(rpc)]
    interface IFungibleBridge {
        function blockBurn(bytes calldata cert, uint32 txIndex, uint32 eventPosInTx) external;
        function isBurnBlocked(uint64 height, uint32 eventIndex) external view returns (bool);
        function isBurnProcessed(uint64 height, uint32 eventIndex) external view returns (bool);
    }
}

/// Initial wrapped-token balance minted to `owner_b` on chain B. Sized
/// large enough that the post-burn pre-refund snapshot is non-zero and
/// the post-refund snapshot must equal it exactly.
const INITIAL_BALANCE_TOKENS: u128 = 100;
/// One whole wrapped token (1e18 sub-units) — the smallest realistic
/// burn that still exercises the full encoder path.
const BURN_AMOUNT_TOKENS: u128 = 1;

/// Locates the burn `BurnEvent` for `fungible_app_id` inside a confirmed
/// block. Returns `(tx_index, event_pos_in_tx, event_index, burn)` for
/// the first match. The test triggers exactly one burn so this is the
/// only event of interest.
///
/// Mirrors `linera_bridge::relay::linera::find_burn_events` (which is
/// `pub(crate)`) — duplicated here intentionally instead of widening
/// the public surface for a single test.
fn find_first_burn(
    events: &[Vec<Event>],
    fungible_app_id: ApplicationId,
) -> Option<(u32, u32, u32, BurnEvent)> {
    for (tx_index, tx_events) in (0u32..).zip(events) {
        for (event_pos, event) in (0u32..).zip(tx_events) {
            if event.stream_id.application_id != GenericApplicationId::User(fungible_app_id) {
                continue;
            }
            if event.stream_id.stream_name.0 != b"burns" {
                continue;
            }
            if let Ok(burn) = bcs::from_bytes::<BurnEvent>(&event.value) {
                return Some((tx_index, event_pos, event.index, burn));
            }
        }
    }
    None
}

/// Queries the wrapped-fungible service for `owner`'s balance on the
/// chain the `chain_client` is bound to. Returns 0 (`U128(0)`) when the
/// account entry is absent rather than failing — matches the
/// wrapped-fungible semantics for uncredited accounts.
async fn query_wrapped_balance<E>(
    chain_client: &linera_core::client::ChainClient<E>,
    fungible_app_id: ApplicationId,
    owner: AccountOwner,
) -> anyhow::Result<U128>
where
    E: linera_core::environment::Environment,
{
    #[derive(serde::Serialize)]
    struct GqlRequest {
        query: String,
    }

    let gql = format!(r#"query {{ accounts {{ entry(key: "{owner}") {{ value }} }} }}"#);
    let query = Query::user_without_abi(fungible_app_id, &GqlRequest { query: gql })?;
    let (outcome, _) = chain_client.query_application(query, None).await?;
    let response_bytes = match outcome.response {
        QueryResponse::User(bytes) => bytes,
        other => anyhow::bail!("unexpected query response: {other:?}"),
    };
    let response: serde_json::Value = serde_json::from_slice(&response_bytes)?;
    match response["data"]["accounts"]["entry"]["value"].as_str() {
        Some(balance_str) => balance_str.parse::<U128>().context("parse U128"),
        None => Ok(U128(0)),
    }
}

#[tokio::test]
#[ignore] // Requires pre-built docker images, Wasm, and relay binary.
async fn relayer_refunds_blocked_burn() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_test_writer().try_init().ok();
    linera_bridge_e2e::ensure_rustls_provider();
    let compose_file = compose_file_path();
    let project_name = "linera-refund-after-block-test";

    let compose = start_compose(&compose_file, project_name).await;
    wait_for_light_client(&compose, project_name, &compose_file).await;

    let faucet = Faucet::new("http://localhost:8080".to_string());
    let genesis_config = faucet.genesis_config().await?;
    let relay_genesis_config = genesis_config.clone();

    let store_config = MemoryStoreConfig {
        max_stream_queries: 10,
        kill_on_drop: true,
    };
    let mut storage = DbStorage::<MemoryDatabase, _>::maybe_create_and_connect(
        &store_config,
        "refund-after-block-e2e",
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

    // Chain A hosts both the evm-bridge and the wrapped-fungible apps
    // and is the chain the relayer drives. Chain B is the user chain
    // that initiates the burn.
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

    // ── Deploy LineraToken (EVM) ──
    let erc20_addr = deploy_linera_token(&compose, project_name, &compose_file).await?;

    // ── Publish & deploy the evm-bridge Wasm app on chain A ──
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .context("manifest dir has fewer than 3 ancestors")?
        .to_path_buf();
    let evm_bridge_wasm_dir =
        repo_root.join("linera-bridge/contracts/evm-bridge/target/wasm32-unknown-unknown/release");
    let wasm_dir = repo_root.join("examples/target/wasm32-unknown-unknown/release");

    let eb_contract =
        Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_contract.wasm"))?;
    let eb_service = Bytecode::load_from_file(evm_bridge_wasm_dir.join("evm_bridge_service.wasm"))?;
    let (eb_module_id, _) = cc_a
        .publish_module(eb_contract, eb_service, VmRuntime::Wasm)
        .await?
        .expect("publish evm-bridge module committed");
    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    let (bridge_app_id, _) = cc_a
        .create_application_untyped(
            eb_module_id,
            serde_json::to_vec(&BridgeParameters {
                source_chain_id: 31337,
                token_address: erc20_addr.0 .0,
            })?,
            serde_json::to_vec(&BridgeInstantiationArgument {
                rpc_endpoint: String::new(),
            })?,
            vec![],
        )
        .await?
        .expect("create evm-bridge app committed");

    // ── Publish & create wrapped-fungible app on chain A, pre-funding
    //    owner_b on chain B with INITIAL_BALANCE_TOKENS so the burn
    //    leaves a deterministic shortfall the refund must restore. ──
    let wf_contract = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_contract.wasm"))?;
    let wf_service = Bytecode::load_from_file(wasm_dir.join("wrapped_fungible_service.wasm"))?;
    let (wf_module_id, _) = cc_a
        .publish_module(wf_contract, wf_service, VmRuntime::Wasm)
        .await?
        .expect("publish wrapped-fungible module committed");
    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;

    let initial_balance = U128(INITIAL_BALANCE_TOKENS * 10u128.pow(18));
    let (fungible_app_id, _) = cc_a
        .create_application_untyped(
            wf_module_id,
            serde_json::to_vec(&WrappedParameters {
                ticker_symbol: "wTEST".to_string(),
                decimals: 18,
                minter: Some(owner_a),
                mint_chain_id: Some(chain_a),
                evm_token_address: erc20_addr.0 .0,
                evm_source_chain_id: 31337,
                bridge_app_id: Some(bridge_app_id),
            })?,
            serde_json::to_vec(&InitialState {
                accounts: BTreeMap::from([(owner_b, initial_balance)]),
            })?,
            vec![],
        )
        .await?
        .expect("create wrapped-fungible app committed");

    // Register the wrapped-fungible app on the evm-bridge so RefundBurn
    // can call back into it.
    let register_fungible_op = Operation::User {
        application_id: bridge_app_id,
        bytes: bcs::to_bytes(&BridgeOperation::RegisterFungibleApp {
            app_id: fungible_app_id,
        })?,
    };
    cc_a.execute_operations(vec![register_fungible_op], vec![])
        .await?
        .expect("register fungible app committed");

    // ── Deploy FungibleBridge on EVM with the real wrapped-fungible
    //    app ID baked into the constructor. ──
    let app_id_bytes32 = format!("0x{}", fungible_app_id.application_description_hash);
    let chain_a_bytes32 = format!("0x{chain_a}");
    let bridge_addr = deploy_fungible_bridge(
        &compose,
        project_name,
        &compose_file,
        light_client_address(),
        &chain_a_bytes32,
        erc20_addr,
        &app_id_bytes32,
    )
    .await?;

    // Fund the bridge so an unexpected `_onBlock` release would succeed
    // — eliminates "insufficient ERC-20" as an alternative explanation
    // for the EVM recipient's zero balance assertion below.
    fund_bridge_erc20(
        &compose,
        project_name,
        &compose_file,
        erc20_addr,
        bridge_addr,
        1_000_000_000_000_000_000_000u128,
    )
    .await;

    // Register the FungibleBridge address on the evm-bridge so the
    // RefundBurn proof verification can match the log emitter.
    let register_bridge_op = Operation::User {
        application_id: bridge_app_id,
        bytes: bcs::to_bytes(&BridgeOperation::RegisterFungibleBridge {
            address: bridge_addr.0 .0,
        })?,
    };
    cc_a.execute_operations(vec![register_bridge_op], vec![])
        .await?
        .expect("register bridge address committed");

    // ── Trigger the burn: chain B does a cross-chain Transfer to an
    //    Address20 owner on chain A. Chain A's wrapped-fungible app
    //    receives the Credit and emits a BurnEvent on the "burns"
    //    stream. ──
    let evm_recipient: Address = "0x70997970C51812dc3A010C7d01b50e0d17dc79C8".parse()?;
    let evm_recipient_owner = AccountOwner::Address20(evm_recipient.0 .0);
    let burn_amount = U128(BURN_AMOUNT_TOKENS * 10u128.pow(18));

    cc_b.synchronize_from_validators().await?;
    let transfer_bytes = bcs::to_bytes(&WrappedFungibleOperation::Transfer {
        owner: owner_b,
        amount: burn_amount,
        target_account: Account {
            chain_id: chain_a,
            owner: evm_recipient_owner,
        },
    })?;
    cc_b.execute_operations(
        vec![Operation::User {
            application_id: fungible_app_id,
            bytes: transfer_bytes,
        }],
        vec![],
    )
    .await?
    .expect("cross-chain burn committed on chain B");

    // Snapshot owner_b's balance immediately after the burn debit —
    // this is the pre-refund baseline. The refund must restore it back
    // to `initial_balance`, recovering exactly `burn_amount`.
    let post_burn_balance = query_wrapped_balance(&cc_b, fungible_app_id, owner_b).await?;
    assert_eq!(
        post_burn_balance,
        U128(initial_balance.0 - burn_amount.0),
        "post-burn balance must reflect the debit"
    );

    // Drive chain A's inbox so the Credit message materialises as a
    // BurnEvent cert on chain A.
    cc_a.synchronize_from_validators().await?;
    cc_a.process_inbox().await?;
    cc_a.synchronize_from_validators().await?;

    let burn_cert = fetch_latest_cert(&cc_a).await?;
    let burn_block = burn_cert.block();
    let burn_height = burn_block.header.height;
    let (tx_index, event_pos_in_tx, event_index, burn_event) =
        find_first_burn(&burn_block.body.events, fungible_app_id)
            .expect("burn event must be present in chain A's latest cert");

    assert_eq!(
        burn_event.amount, burn_amount,
        "decoded burn amount mismatch"
    );
    assert_eq!(
        burn_event.source.chain_id, chain_b,
        "burn source chain id mismatch"
    );
    assert_eq!(
        burn_event.source.owner, owner_b,
        "burn source owner mismatch"
    );

    // ── Call `blockBurn` directly from the test signer BEFORE
    //    spawning the relayer, so the relayer never sees an unblocked
    //    burn cert. The cert payload is the same BCS-serialized
    //    `ConfirmedBlockCertificate` the relayer ships to `addBlock`. ──
    let rpc_url = "http://localhost:8545".parse()?;
    let test_signer: PrivateKeySigner = ANVIL_PRIVATE_KEY.parse()?;
    let test_signer_addr = test_signer.address();
    let wallet = EthereumWallet::from(test_signer);
    let provider = ProviderBuilder::new().wallet(wallet).connect_http(rpc_url);
    let bridge = IFungibleBridge::new(bridge_addr, &provider);

    let cert_bytes = bcs::to_bytes(burn_cert.as_ref())
        .context("BCS-serialize ConfirmedBlockCertificate for blockBurn")?;
    let block_burn_receipt = bridge
        .blockBurn(cert_bytes.into(), tx_index, event_pos_in_tx)
        .send()
        .await?
        .get_receipt()
        .await?;
    assert!(block_burn_receipt.status(), "blockBurn tx must succeed");

    // Locate the BurnBlocked log in the receipt and parse it with the
    // same routine the relayer uses, then check every decoded field
    // against the burn certificate so a regression in either the
    // emitter or the parser surfaces here.
    let event_sig = linera_bridge::proof::burn_blocked_event_signature();
    let (log_index, alloy_log) = block_burn_receipt
        .inner
        .logs()
        .iter()
        .enumerate()
        .find(|(_, log)| log.address() == bridge_addr && log.topic0() == Some(&event_sig))
        .map(|(i, log)| (i as u64, log))
        .expect("BurnBlocked log not found in blockBurn receipt");

    let receipt_log = ReceiptLog {
        address: alloy_log.address(),
        topics: alloy_log.data().topics().to_vec(),
        data: alloy_log.data().data.to_vec(),
    };
    let fields = parse_burn_blocked_event(&receipt_log, bridge_addr)
        .context("parse BurnBlocked log")?;
    assert_eq!(
        fields.height, burn_height.0,
        "BurnBlocked.height mismatch"
    );
    assert_eq!(
        fields.event_index, event_index,
        "BurnBlocked.eventIndex mismatch"
    );
    assert_eq!(
        fields.blocked_by, test_signer_addr,
        "BurnBlocked.blocked_by must match the caller"
    );
    assert_eq!(
        fields.source_chain_id, chain_b,
        "BurnBlocked.source_chain_id must equal chain B"
    );
    assert_eq!(
        fields.source_owner, owner_b,
        "BurnBlocked.source_owner must BCS-decode to chain B's burner"
    );
    assert_eq!(
        fields.amount.to_attos(),
        burn_amount.0,
        "BurnBlocked.amount mismatch"
    );

    // Sanity: contract views agree with the event.
    assert!(
        bridge
            .isBurnBlocked(burn_height.0, event_index)
            .call()
            .await?,
        "isBurnBlocked must be true after blockBurn"
    );
    assert!(
        !bridge
            .isBurnProcessed(burn_height.0, event_index)
            .call()
            .await?,
        "isBurnProcessed must remain false — release was blocked"
    );

    // Snapshot the BurnBlocked tx coordinates for the RefundKey poll.
    let refund_key = RefundKey {
        source_chain_id: 31337,
        block_hash: block_burn_receipt.block_hash.context("block_hash missing")?,
        tx_index: block_burn_receipt
            .transaction_index
            .context("transaction_index missing")?,
        log_index,
    };

    // ── Now spawn the relayer. Its EVM scanner picks up the
    //    BurnBlocked log, builds a refund proof, and submits
    //    RefundBurn on the evm-bridge app. ──
    let relay_dir = tempfile::tempdir()?;
    let wallet_path = relay_dir.path().join("wallet.json");
    let keystore_path = relay_dir.path().join("keystore.json");
    let storage_config = format!("rocksdb:{}", relay_dir.path().join("client.db").display());
    let sqlite_path = relay_dir.path().join("relay.sqlite3");

    {
        use linera_persistent::Persist;
        let mut ks = linera_persistent::File::new(&keystore_path, signer.clone())?;
        ks.persist().await?;
    }
    linera_wallet_json::PersistentWallet::create(&wallet_path, relay_genesis_config)?;

    let relay_port = 3006u16;
    let bridge_addr_str = format!("{bridge_addr}");
    let bridge_app_str = format!("{bridge_app_id}");
    let fungible_app_str = format!("{fungible_app_id}");
    let sqlite_path_for_relay = sqlite_path.clone();
    let relay_handle = tokio::spawn(async move {
        Box::pin(linera_bridge::relay::run(
            "http://localhost:8545",
            Some(wallet_path.as_path()),
            Some(keystore_path.as_path()),
            Some(&storage_config),
            chain_a,
            owner_a,
            &bridge_addr_str,
            &bridge_app_str,
            &fungible_app_str,
            ANVIL_PRIVATE_KEY,
            None,
            relay_port,
            &linera_storage_runtime::CommonStorageOptions::with_defaults(),
            Duration::from_secs(2),
            0,
            5,
            Some(sqlite_path_for_relay.as_path()),
        ))
        .await
    });

    let relay_url = format!("http://localhost:{relay_port}");
    let http = reqwest::Client::new();
    if let Err(error) = wait_for_relay_http_ready(&http, &relay_url, Duration::from_secs(60)).await
    {
        relay_handle.abort();
        return Err(error);
    }

    // Poll for refund completion on the bridge chain. The refund
    // pipeline is purely on-chain once the relayer submits the proof,
    // so `isRefundProcessed` is the authoritative success signal.
    let refund_deadline = std::time::Instant::now() + Duration::from_secs(120);
    let mut refund_processed = false;
    while std::time::Instant::now() < refund_deadline {
        if relay_handle.is_finished() {
            anyhow::bail!("Relay exited unexpectedly: {:?}", relay_handle.await);
        }
        // Sync chain B so the cross-chain Mint message lands and the
        // balance assertion below sees the refunded credit.
        cc_b.synchronize_from_validators().await?;
        cc_b.process_inbox().await?;
        match linera_bridge::monitor::query_refund_processed(&cc_a, bridge_app_id, &refund_key)
            .await
        {
            Ok(true) => {
                refund_processed = true;
                break;
            }
            Ok(false) => {}
            Err(error) => tracing::warn!(?error, "query_refund_processed failed; retrying"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    if !refund_processed {
        relay_handle.abort();
        anyhow::bail!("Refund was not processed within 120s");
    }

    // One more sync so the Mint cross-chain message is definitely
    // applied to chain B's wrapped-fungible state before we read it.
    cc_b.synchronize_from_validators().await?;
    cc_b.process_inbox().await?;

    let final_balance = query_wrapped_balance(&cc_b, fungible_app_id, owner_b).await?;
    let token = IERC20::new(erc20_addr, &provider);
    let evm_recipient_balance = token.balanceOf(evm_recipient).call().await?;
    let still_blocked = bridge
        .isBurnBlocked(burn_height.0, event_index)
        .call()
        .await?;
    let processed = bridge
        .isBurnProcessed(burn_height.0, event_index)
        .call()
        .await?;
    let final_metrics = http
        .get(format!("{relay_url}/metrics"))
        .send()
        .await?
        .text()
        .await?;
    let refunds_completed = parse_metric_value(&final_metrics, "linera_bridge_refunds_completed");

    relay_handle.abort();

    assert_eq!(
        final_balance, initial_balance,
        "owner_b's wrapped balance must return to the pre-burn level"
    );
    assert_eq!(
        evm_recipient_balance,
        U256::ZERO,
        "EVM recipient must hold no ERC-20 — _onBlock was blocked from releasing"
    );
    assert!(
        still_blocked,
        "isBurnBlocked must remain true after the refund"
    );
    assert!(
        !processed,
        "isBurnProcessed must remain false — refund does not flip the release flag"
    );
    assert!(
        refunds_completed >= 1,
        "linera_bridge_refunds_completed must reach 1; got {refunds_completed}"
    );

    Ok(())
}
