// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]

#[cfg(feature = "jemalloc")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

// jemalloc configuration for memory profiling with jemalloc_pprof
// prof:true,prof_active:true - Enable profiling from start
// lg_prof_sample:19 - Sample every 512KB for good detail/overhead balance

// Linux/other platforms: use unprefixed malloc (with unprefixed_malloc_on_supported_platforms)
#[cfg(all(feature = "memory-profiling", not(target_os = "macos")))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

// macOS: use prefixed malloc (without unprefixed_malloc_on_supported_platforms)
#[cfg(all(feature = "memory-profiling", target_os = "macos"))]
#[allow(non_upper_case_globals)]
#[export_name = "_rjem_malloc_conf"]
pub static malloc_conf: &[u8] = b"prof:true,prof_active:true,lg_prof_sample:19\0";

mod options;
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    path::PathBuf,
    process,
    sync::Arc,
};

use anyhow::{bail, ensure, Context, Error};
use async_trait::async_trait;
use chrono::Utc;
use clap_complete::generate;
use colored::Colorize;
use futures::{lock::Mutex, FutureExt as _, StreamExt as _};
use linera_base::{
    crypto::Signer,
    data_types::{ApplicationPermissions, Timestamp},
    identifiers::{AccountOwner, ChainId},
    listen_for_shutdown_signals,
    ownership::ChainOwnership,
    time::{Duration, Instant},
};
use linera_client::{
    benchmark::BenchmarkConfig,
    chain_listener::{ChainListener, ChainListenerConfig, ClientContext as _},
    config::{CommitteeConfig, GenesisConfig},
};
use linera_core::{
    client::{chain_client, ListeningMode},
    data_types::ClientOutcome,
    node::{ValidatorNode, ValidatorNodeProvider},
    wallet,
    worker::Reason,
    JoinSetExt as _, LocalNodeError,
};
use linera_execution::committee::Committee;
use linera_faucet_server::{FaucetConfig, FaucetService};
#[cfg(with_metrics)]
use linera_metrics::monitoring_server;
use linera_persistent::{self as persistent, Persist, PersistExt as _};
use linera_service::{
    cli::{
        command::{
            BenchmarkCommand, BenchmarkOptions, ChainCommand, ClientCommand, DatabaseToolCommand,
            NetCommand, ProjectCommand, WalletCommand,
        },
        net_up_utils,
    },
    cli_wrappers::{self, local_net::PathProvider, ClientWrapper, Network, OnClientDrop},
    controller::Controller,
    node_service::NodeService,
    project::{self, Project},
    storage::{Runnable, RunnableWithStore},
    task_processor::TaskProcessor,
    util,
};
use linera_storage::{DbStorage, Storage};
use linera_views::store::{KeyValueDatabase, KeyValueStore};
use options::Options;
use serde_json::Value;
use tempfile::NamedTempFile;
use tokio::{
    io::AsyncWriteExt,
    process::{ChildStdin, Command},
    sync::{mpsc, oneshot},
    task::JoinSet,
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument as _};

struct Job(Options);

/// Check if an error is retryable (HTTP 502, 503, 504, timeouts, connection errors)
fn is_retryable_error(err: &anyhow::Error) -> bool {
    // Check for reqwest errors in the error chain
    if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>() {
        // Check for retryable HTTP status codes (502, 503, 504)
        if let Some(status) = reqwest_err.status() {
            return status == reqwest::StatusCode::BAD_GATEWAY
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE
                || status == reqwest::StatusCode::GATEWAY_TIMEOUT;
        }
        // Check for connection errors or timeouts
        return reqwest_err.is_timeout() || reqwest_err.is_connect();
    }
    false
}

/// Retry a faucet operation with exponential backoff
async fn retry_faucet_operation<F, Fut, T>(operation: F) -> anyhow::Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<T>>,
{
    let max_retries = 5;
    let mut attempt = 0;

    loop {
        attempt += 1;
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) if attempt < max_retries && is_retryable_error(&err) => {
                let backoff_ms = 100 * 2_u64.pow(attempt - 1);
                warn!(
                    "Faucet operation failed with retryable error (attempt {}/{}): {:?}. Retrying after {}ms",
                    attempt, max_retries, err, backoff_ms
                );
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
            Err(err) => return Err(err),
        }
    }
}

fn read_json(string: Option<String>, path: Option<PathBuf>) -> anyhow::Result<Vec<u8>> {
    let value = match (string, path) {
        (Some(_), Some(_)) => bail!("cannot have both a json string and file"),
        (Some(s), None) => serde_json::from_str(&s)?,
        (None, Some(path)) => {
            let s = fs_err::read_to_string(path)?;
            serde_json::from_str(&s)?
        }
        (None, None) => Value::Null,
    };
    Ok(serde_json::to_vec(&value)?)
}

#[async_trait]
impl Runnable for Job {
    type Output = anyhow::Result<()>;

    async fn run<S>(self, storage: S) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let Job(options) = self;
        let mut wallet = options.wallet()?;
        let mut signer = options.signer()?;

        let command = options.command.clone();

        use ClientCommand::*;
        match command {
            Transfer {
                sender,
                recipient,
                amount,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_client = context.make_chain_client(sender.chain_id).await?;
                info!(
                    "Starting transfer of {} native tokens from {} to {}",
                    amount, sender, recipient
                );
                let time_start = Instant::now();
                let certificate = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        async move {
                            chain_client
                                .transfer_to_account(sender.owner, amount, recipient)
                                .await
                        }
                    })
                    .await
                    .context("Failed to make transfer")?;
                let time_total = time_start.elapsed();
                info!("Transfer confirmed after {} ms", time_total.as_millis());
                debug!("{:?}", certificate);
            }

            OpenChain {
                chain_id,
                owner,
                balance,
                super_owner,
            } => {
                let new_owner = owner.unwrap_or_else(|| signer.generate_new().into());
                signer.persist().await?;
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).await?;
                info!("Opening a new chain from existing chain {}", chain_id);
                let time_start = Instant::now();
                let (description, certificate) = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let ownership = if super_owner {
                            ChainOwnership::single_super(new_owner)
                        } else {
                            ChainOwnership::single(new_owner)
                        };

                        let chain_client = chain_client.clone();
                        async move {
                            chain_client
                                .open_chain(ownership, ApplicationPermissions::default(), balance)
                                .await
                        }
                    })
                    .await
                    .context("Failed to open chain")?;
                let timestamp = certificate.block().header.timestamp;
                let epoch = certificate.block().header.epoch;
                let id = description.id();
                context
                    .update_wallet_for_new_chain(id, Some(new_owner), timestamp, epoch)
                    .await?;
                let time_total = time_start.elapsed();
                info!(
                    "Opening a new chain confirmed after {} ms",
                    time_total.as_millis()
                );
                debug!("{:?}", certificate);
                // Print the new chain ID, and owner on stdout for scripting purposes.
                println!("{}", id);
                println!("{}", new_owner);
            }

            OpenMultiOwnerChain {
                chain_id,
                balance,
                ownership_config,
                application_permissions_config,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).await?;
                info!(
                    "Opening a new multi-owner chain from existing chain {}",
                    chain_id
                );
                let time_start = Instant::now();
                let ownership = ChainOwnership::try_from(ownership_config)?;
                let mut application_permissions = ApplicationPermissions::default();
                application_permissions_config.update(&mut application_permissions);
                let (description, certificate) = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let ownership = ownership.clone();
                        let application_permissions = application_permissions.clone();
                        let chain_client = chain_client.clone();
                        async move {
                            chain_client
                                .open_chain(ownership, application_permissions, balance)
                                .await
                        }
                    })
                    .await
                    .context("Failed to open chain")?;
                let id = description.id();
                // No owner. This chain can be assigned explicitly using the assign command.
                let owner = None;
                let timestamp = certificate.block().header.timestamp;
                let epoch = certificate.block().header.epoch;
                context
                    .update_wallet_for_new_chain(id, owner, timestamp, epoch)
                    .await?;
                let time_total = time_start.elapsed();
                info!(
                    "Opening a new multi-owner chain confirmed after {} ms",
                    time_total.as_millis()
                );
                debug!("{:?}", certificate);
                // Print the new chain ID on stdout for scripting purposes.
                println!("{}", id);
            }

            ShowOwnership { chain_id } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let ownership = context.ownership(chain_id).await?;
                let json = serde_json::to_string_pretty(&ownership)?;
                println!("{}", json);
            }

            ChangeOwnership {
                chain_id,
                ownership_config,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                context.change_ownership(chain_id, ownership_config).await?
            }

            SetPreferredOwner { chain_id, owner } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                context.set_preferred_owner(chain_id, owner).await?
            }

            ChangeApplicationPermissions {
                chain_id,
                application_permissions_config,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).await?;
                info!("Changing application permissions for chain {}", chain_id);
                let time_start = Instant::now();
                let certificate = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let application_permissions_config = application_permissions_config.clone();
                        let chain_client = chain_client.clone();
                        async move {
                            let mut application_permissions =
                                chain_client.query_application_permissions().await?;
                            application_permissions_config.update(&mut application_permissions);
                            chain_client
                                .change_application_permissions(application_permissions)
                                .await
                        }
                    })
                    .await
                    .context("Failed to change application permissions")?;
                let time_total = time_start.elapsed();
                info!(
                    "Changing application permissions confirmed after {} ms",
                    time_total.as_millis()
                );
                debug!("{:?}", certificate);
            }

            CloseChain { chain_id } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_client = context.make_chain_client(chain_id).await?;
                info!("Closing chain {}", chain_id);
                let time_start = Instant::now();
                let result = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        async move { chain_client.close_chain().await }
                    })
                    .await;
                let certificate = match result {
                    Ok(Some(certificate)) => certificate,
                    Ok(None) => {
                        info!("Chain is already closed; nothing to do.");
                        return Ok(());
                    }
                    Err(error) => Err(error).context("Failed to close chain")?,
                };
                let time_total = time_start.elapsed();
                info!(
                    "Closing chain confirmed after {} ms",
                    time_total.as_millis()
                );
                debug!("{:?}", certificate);
            }

            ShowNetworkDescription => {
                let network_description = storage.read_network_description().await?;
                let json = serde_json::to_string_pretty(&network_description)?;
                println!("{}", json);
            }

            LocalBalance { account } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let account = account.unwrap_or_else(|| context.default_account());
                let chain_client = context.make_chain_client(account.chain_id).await?;
                info!("Reading the balance of {} from the local state", account);
                let time_start = Instant::now();
                let balance = chain_client.local_owner_balance(account.owner).await?;
                let time_total = time_start.elapsed();
                info!("Local balance obtained after {} ms", time_total.as_millis());
                println!("{}", balance);
            }

            QueryBalance { account } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let account = account.unwrap_or_else(|| context.default_account());
                let chain_client = context.make_chain_client(account.chain_id).await?;
                info!(
                    "Evaluating the local balance of {account} by staging execution of known \
                    incoming messages"
                );
                let time_start = Instant::now();
                let balance = chain_client.query_owner_balance(account.owner).await?;
                let time_total = time_start.elapsed();
                info!("Balance obtained after {} ms", time_total.as_millis());
                println!("{}", balance);
            }

            SyncBalance { account } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let account = account.unwrap_or_else(|| context.default_account());
                let chain_client = context.make_chain_client(account.chain_id).await?;
                info!("Synchronizing chain information and querying the local balance");
                warn!("This command is deprecated. Use `linera sync && linera query-balance` instead.");
                let time_start = Instant::now();
                chain_client.synchronize_from_validators().await?;
                let result = chain_client.query_owner_balance(account.owner).await;
                context.update_wallet_from_client(&chain_client).await?;
                let balance = result.context("Failed to synchronize from validators")?;
                let time_total = time_start.elapsed();
                info!(
                    "Synchronizing balance confirmed after {} ms",
                    time_total.as_millis()
                );
                println!("{}", balance);
            }

            Sync { chain_id } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).await?;
                info!("Synchronizing chain information");
                let time_start = Instant::now();
                chain_client.synchronize_from_validators().await?;
                context.update_wallet_from_client(&chain_client).await?;
                let time_total = time_start.elapsed();
                info!(
                    "Synchronized chain information in {} ms",
                    time_total.as_millis()
                );
            }

            ProcessInbox { chain_id } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let follow_only = context
                    .wallet()
                    .get(chain_id)
                    .is_some_and(|chain| chain.is_follow_only());
                if follow_only {
                    anyhow::bail!(
                        "Cannot process inbox for follow-only chain {chain_id}. \
                         Use `linera assign` to take ownership of the chain first."
                    );
                }
                let chain_client = context.make_chain_client(chain_id).await?;
                info!("Processing the inbox of chain {}", chain_id);
                let time_start = Instant::now();
                let certificates = context.process_inbox(&chain_client).await?;
                let time_total = time_start.elapsed();
                info!(
                    "Processed incoming messages with {} blocks in {} ms",
                    certificates.len(),
                    time_total.as_millis()
                );
            }

            QueryShardInfo { chain_id } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                println!("Querying validators for shard information about chain {chain_id}.\n");
                let chain_client = context.make_chain_client(chain_id).await?;
                let result = chain_client.local_committee().await;
                context.update_wallet_from_client(&chain_client).await?;
                let committee = result.context("Failed to get local committee")?;
                let node_provider = context.make_node_provider();

                println!("Chain ID: {}", chain_id);
                println!("Validator Shard Information:\n");

                for (name, state) in committee.validators() {
                    let address = &state.network_address;
                    let node = node_provider.make_node(address)?;

                    match node.get_shard_info(chain_id).await {
                        Ok(shard_info) => {
                            println!("  Validator: {}", name);
                            println!("    Address: {}", address);
                            println!("    Total Shards: {}", shard_info.total_shards);
                            println!("    Shard ID for chain: {}", shard_info.shard_id);
                            println!();
                        }
                        Err(e) => {
                            println!("  Validator: {}", name);
                            println!("    Address: {}", address);
                            println!("    Error: Failed to get shard info - {}", e);
                            println!();
                        }
                    }
                }
            }

            command @ ResourceControlPolicy { .. } => {
                info!("Starting operations to change resource control policy");

                let time_start = Instant::now();
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                // ResourceControlPolicy doesn't need version checks
                let admin_chain_id = context.admin_chain_id();
                let chain_client = context.make_chain_client(admin_chain_id).await?;
                // Synchronize the chain state to make sure we're applying the changes to the
                // latest committee.
                chain_client.synchronize_chain_state(admin_chain_id).await?;
                let maybe_certificate = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        let command = command.clone();
                        async move {
                            // Update resource control policy
                            let mut committee = chain_client.local_committee().await.unwrap();
                            let mut policy = committee.policy().clone();
                            let validators = committee.validators().clone();
                            match command {
                                ResourceControlPolicy {
                                    wasm_fuel_unit,
                                    evm_fuel_unit,
                                    read_operation,
                                    write_operation,
                                    byte_runtime,
                                    byte_read,
                                    byte_written,
                                    blob_read,
                                    blob_published,
                                    blob_byte_read,
                                    blob_byte_published,
                                    byte_stored,
                                    operation,
                                    operation_byte,
                                    message,
                                    message_byte,
                                    service_as_oracle_query,
                                    http_request,
                                    maximum_wasm_fuel_per_block,
                                    maximum_evm_fuel_per_block,
                                    maximum_service_oracle_execution_ms,
                                    maximum_block_size,
                                    maximum_blob_size,
                                    maximum_published_blobs,
                                    maximum_bytecode_size,
                                    maximum_block_proposal_size,
                                    maximum_bytes_read_per_block,
                                    maximum_bytes_written_per_block,
                                    maximum_oracle_response_bytes,
                                    maximum_http_response_bytes,
                                    http_request_timeout_ms,
                                    http_request_allow_list,
                                } => {
                                    let existing_policy = policy.clone();
                                    policy = linera_execution::ResourceControlPolicy {
                                        wasm_fuel_unit: wasm_fuel_unit
                                            .unwrap_or(existing_policy.wasm_fuel_unit),
                                        evm_fuel_unit: evm_fuel_unit
                                            .unwrap_or(existing_policy.evm_fuel_unit),
                                        read_operation: read_operation
                                            .unwrap_or(existing_policy.read_operation),
                                        write_operation: write_operation
                                            .unwrap_or(existing_policy.write_operation),
                                        byte_runtime: byte_runtime
                                            .unwrap_or(existing_policy.byte_runtime),
                                        byte_read: byte_read.unwrap_or(existing_policy.byte_read),
                                        byte_written: byte_written
                                            .unwrap_or(existing_policy.byte_written),
                                        blob_read: blob_read.unwrap_or(existing_policy.blob_read),
                                        blob_published: blob_published
                                            .unwrap_or(existing_policy.blob_published),
                                        blob_byte_read: blob_byte_read
                                            .unwrap_or(existing_policy.blob_byte_read),
                                        blob_byte_published: blob_byte_published
                                            .unwrap_or(existing_policy.blob_byte_published),
                                        byte_stored: byte_stored
                                            .unwrap_or(existing_policy.byte_stored),
                                        operation: operation.unwrap_or(existing_policy.operation),
                                        operation_byte: operation_byte
                                            .unwrap_or(existing_policy.operation_byte),
                                        message: message.unwrap_or(existing_policy.message),
                                        message_byte: message_byte
                                            .unwrap_or(existing_policy.message_byte),
                                        service_as_oracle_query: service_as_oracle_query
                                            .unwrap_or(existing_policy.service_as_oracle_query),
                                        http_request: http_request
                                            .unwrap_or(existing_policy.http_request),
                                        maximum_wasm_fuel_per_block: maximum_wasm_fuel_per_block
                                            .unwrap_or(existing_policy.maximum_wasm_fuel_per_block),
                                        maximum_evm_fuel_per_block: maximum_evm_fuel_per_block
                                            .unwrap_or(existing_policy.maximum_evm_fuel_per_block),
                                        maximum_service_oracle_execution_ms:
                                            maximum_service_oracle_execution_ms.unwrap_or(
                                                existing_policy.maximum_service_oracle_execution_ms,
                                            ),
                                        maximum_block_size: maximum_block_size
                                            .unwrap_or(existing_policy.maximum_block_size),
                                        maximum_bytecode_size: maximum_bytecode_size
                                            .unwrap_or(existing_policy.maximum_bytecode_size),
                                        maximum_blob_size: maximum_blob_size
                                            .unwrap_or(existing_policy.maximum_blob_size),
                                        maximum_published_blobs: maximum_published_blobs
                                            .unwrap_or(existing_policy.maximum_published_blobs),
                                        maximum_block_proposal_size: maximum_block_proposal_size
                                            .unwrap_or(existing_policy.maximum_block_proposal_size),
                                        maximum_bytes_read_per_block: maximum_bytes_read_per_block
                                            .unwrap_or(
                                                existing_policy.maximum_bytes_read_per_block,
                                            ),
                                        maximum_bytes_written_per_block:
                                            maximum_bytes_written_per_block.unwrap_or(
                                                existing_policy.maximum_bytes_written_per_block,
                                            ),
                                        maximum_oracle_response_bytes:
                                            maximum_oracle_response_bytes.unwrap_or(
                                                existing_policy.maximum_oracle_response_bytes,
                                            ),
                                        maximum_http_response_bytes: maximum_http_response_bytes
                                            .unwrap_or(existing_policy.maximum_http_response_bytes),
                                        http_request_timeout_ms: http_request_timeout_ms
                                            .unwrap_or(existing_policy.http_request_timeout_ms),
                                        http_request_allow_list: http_request_allow_list
                                            .map(BTreeSet::from_iter)
                                            .unwrap_or(existing_policy.http_request_allow_list),
                                    };
                                    info!("{policy}");
                                    if committee.policy() == &policy {
                                        return Ok(ClientOutcome::Committed(None));
                                    }
                                }
                                _ => unreachable!(),
                            }
                            committee = Committee::new(validators, policy);
                            chain_client
                                .stage_new_committee(committee)
                                .await
                                .map(|outcome| outcome.map(Some))
                        }
                    })
                    .await
                    .context("Failed to stage committee")?;
                let Some(certificate) = maybe_certificate else {
                    return Ok(());
                };
                info!("Created new committee:\n{:?}", certificate);

                let time_total = time_start.elapsed();
                info!("Operations confirmed after {} ms", time_total.as_millis());
            }

            RevokeEpochs { epoch } => {
                info!("Starting operations to remove old committees");
                let time_start = Instant::now();
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let chain_client = context
                    .make_chain_client(context.wallet().genesis_admin_chain_id())
                    .await?;

                // Remove the old committees.
                info!("Revoking epochs");
                context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        async move { chain_client.revoke_epochs(epoch).await }
                    })
                    .await
                    .context("Failed to finalize committee")?;
                context.wallet().save()?;

                let time_total = time_start.elapsed();
                info!(
                    "Revoking committees confirmed after {} ms",
                    time_total.as_millis()
                );
            }

            Benchmark(benchmark_command) => match benchmark_command {
                BenchmarkCommand::Single {
                    options: benchmark_options,
                } => {
                    let BenchmarkOptions {
                        num_chains,
                        tokens_per_chain,
                        transactions_per_block,
                        fungible_application_id,
                        bps,
                        close_chains,
                        health_check_endpoints,
                        wrap_up_max_in_flight,
                        confirm_before_start,
                        runtime_in_seconds,
                        delay_between_chains_ms,
                        config_path,
                        single_destination_per_block,
                    } = benchmark_options;
                    assert!(
                        options.client_options.max_pending_message_bundles
                            >= transactions_per_block,
                        "max_pending_message_bundles must be set to at least the same as the \
                         number of transactions per block ({transactions_per_block}) for benchmarking",
                    );
                    assert!(num_chains > 0, "Number of chains must be greater than 0");
                    assert!(
                        transactions_per_block > 0,
                        "Number of transactions per block must be greater than 0"
                    );
                    assert!(bps > 0, "BPS must be greater than 0");

                    let listener_config = ChainListenerConfig {
                        skip_process_inbox: true,
                        ..Default::default()
                    };

                    let pub_keys: Vec<_> = std::iter::repeat_with(|| signer.generate_new())
                        .take(num_chains)
                        .collect();
                    signer.persist().await?;

                    let mut context = options
                        .create_client_context(storage.clone(), wallet, signer.into_value())
                        .await?;
                    let (chain_clients, all_chains) = context
                        .prepare_for_benchmark(
                            num_chains,
                            tokens_per_chain,
                            fungible_application_id,
                            pub_keys,
                            config_path.as_deref(),
                        )
                        .await?;

                    if confirm_before_start {
                        info!("Ready to start benchmark. Say 'yes' when you want to proceed. Only 'yes' will be accepted");
                        if !std::io::stdin()
                            .lines()
                            .next()
                            .unwrap()?
                            .eq_ignore_ascii_case("yes")
                        {
                            info!("Benchmark cancelled by user");
                            context
                                .wrap_up_benchmark(
                                    chain_clients,
                                    close_chains,
                                    wrap_up_max_in_flight,
                                )
                                .await?;
                            return Ok(());
                        }
                    }

                    let shutdown_notifier = CancellationToken::new();
                    tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

                    // Start metrics server for benchmark monitoring
                    #[cfg(with_metrics)]
                    {
                        let metrics_address = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
                        monitoring_server::start_metrics(
                            metrics_address,
                            shutdown_notifier.clone(),
                        );
                    }

                    let shared_context = std::sync::Arc::new(futures::lock::Mutex::new(context));
                    let chain_listener = ChainListener::new(
                        listener_config,
                        shared_context.clone(),
                        storage.clone(),
                        shutdown_notifier.clone(),
                        mpsc::unbounded_channel().1,
                        true, // Enabling background sync for benchmarks
                    );
                    linera_client::benchmark::Benchmark::run_benchmark(
                        bps,
                        chain_clients.clone(),
                        all_chains,
                        transactions_per_block,
                        fungible_application_id,
                        health_check_endpoints.clone(),
                        runtime_in_seconds,
                        delay_between_chains_ms,
                        chain_listener,
                        &shutdown_notifier,
                        single_destination_per_block,
                    )
                    .await?;

                    let mut context = std::sync::Arc::try_unwrap(shared_context)
                        .map_err(|_| anyhow::anyhow!("Failed to unwrap shared context"))?
                        .into_inner();
                    context
                        .wrap_up_benchmark(chain_clients, close_chains, wrap_up_max_in_flight)
                        .await?;
                }

                BenchmarkCommand::Multi {
                    options: benchmark_options,
                    processes,
                    faucet,
                    client_state_dir,
                    delay_between_processes,
                    cross_wallet_transfers,
                } => {
                    let mut command = BenchmarkCommand::Single {
                        options: benchmark_options.clone(),
                    };
                    let faucet_client = cli_wrappers::Faucet::new(faucet.clone());
                    let on_drop = if benchmark_options.close_chains {
                        OnClientDrop::CloseChains
                    } else {
                        OnClientDrop::LeakChains
                    };

                    let clients = (0..processes)
                        .map(|n| {
                            let path_provider = if let Some(client_state_dir) = &client_state_dir {
                                PathProvider::from_path_option(&Some(
                                    tempfile::tempdir_in(client_state_dir)?
                                        .keep()
                                        .display()
                                        .to_string(),
                                ))?
                            } else {
                                PathProvider::from_path_option(&client_state_dir)?
                            };
                            Ok(Arc::new(ClientWrapper::new_with_extra_args(
                                path_provider,
                                Network::Grpc,
                                None,
                                n,
                                on_drop,
                                vec![
                                    "--storage-max-stream-queries".to_string(),
                                    "50".to_string(),
                                    "--timings".to_string(),
                                ],
                            )))
                        })
                        .collect::<Result<Vec<_>, anyhow::Error>>()?;

                    info!("Initializing wallets...");
                    let mut join_set = JoinSet::new();

                    if cross_wallet_transfers {
                        for client in &clients {
                            let client = client.clone();
                            let faucet_client = faucet_client.clone();
                            join_set.spawn(async move {
                                retry_faucet_operation(|| client.wallet_init(Some(&faucet_client)))
                                    .await?;
                                retry_faucet_operation(|| {
                                    client.request_chain(&faucet_client, true)
                                })
                                .await?;
                                Ok::<_, anyhow::Error>(())
                            });
                        }

                        join_set
                            .join_all()
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()?;

                        let chains_per_wallet = benchmark_options.num_chains;
                        info!(
                            "Creating {} chains per wallet ({} total chains)...",
                            chains_per_wallet,
                            chains_per_wallet * processes
                        );

                        let mut join_set = JoinSet::new();
                        for client in &clients {
                            let default_chain_id = client.default_chain().ok_or_else(|| {
                                anyhow::anyhow!("No default chain found for client")
                            })?;
                            let client = client.clone();
                            join_set.spawn(async move {
                                let mut chain_ids = Vec::new();
                                for _ in 0..chains_per_wallet {
                                    let (chain_id, _owner) = client
                                        .open_chain_super_owner(
                                            default_chain_id,
                                            None,
                                            benchmark_options.tokens_per_chain,
                                        )
                                        .await?;
                                    chain_ids.push(chain_id);
                                }
                                Ok::<Vec<ChainId>, anyhow::Error>(chain_ids)
                            });
                        }

                        let all_chain_ids = join_set
                            .join_all()
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>();

                        let config = BenchmarkConfig {
                            chain_ids: all_chain_ids,
                        };

                        let (_, path) = NamedTempFile::new()?.keep()?;
                        config.save_to_file(&path)?;
                        info!("Saved chains configuration to {}", path.display());
                        if let BenchmarkCommand::Single { options } = &mut command {
                            options.config_path = Some(path);
                        }
                    } else {
                        for client in clients.clone() {
                            let faucet_client = faucet_client.clone();
                            join_set.spawn(async move {
                                retry_faucet_operation(|| client.wallet_init(Some(&faucet_client)))
                                    .await?;
                                retry_faucet_operation(|| {
                                    client.request_chain(&faucet_client, true)
                                })
                                .await?;
                                Ok::<_, anyhow::Error>(())
                            });
                        }

                        join_set
                            .join_all()
                            .await
                            .into_iter()
                            .collect::<Result<Vec<_>, _>>()?;
                    }

                    info!("Starting benchmark processes...");
                    let mut join_set = JoinSet::new();
                    for client in clients.clone() {
                        let command = command.clone();
                        let (tx, rx) = oneshot::channel();
                        join_set.spawn(async move {
                            let result = client.benchmark_detached(command, tx).await?;
                            Ok::<_, anyhow::Error>((result, rx))
                        });
                    }

                    let results = join_set
                        .join_all()
                        .await
                        .into_iter()
                        .collect::<Result<Vec<_>, _>>()?;

                    let mut children = Vec::new();
                    let mut stdout_handles = Vec::new();
                    let mut stderr_handles = Vec::new();
                    let mut rx_handles = Vec::new();
                    for ((child, stdout_handle, stderr_handle), rx) in results {
                        children.push(child);
                        stdout_handles.push(stdout_handle);
                        stderr_handles.push(stderr_handle);
                        rx_handles.push(rx);
                    }

                    if benchmark_options.confirm_before_start {
                        info!("Waiting until all child processes are ready...");
                        let mut ready_count = 0;
                        for rx in rx_handles {
                            rx.await?;
                            ready_count += 1;
                            info!("{}/{} child processes are ready", ready_count, processes);
                        }

                        info!("Ready to start benchmark. Say 'yes' when you want to proceed. Only 'yes' will be accepted");
                        if !std::io::stdin()
                            .lines()
                            .next()
                            .unwrap()?
                            .eq_ignore_ascii_case("yes")
                        {
                            info!("Benchmark cancelled by user");
                            let mut join_set = JoinSet::new();
                            for mut child in children {
                                let mut stdin: ChildStdin = child.stdin.take().unwrap();
                                stdin.write_all(b"no\n").await?;
                                stdin.flush().await?;
                                join_set.spawn(async move {
                                    child.wait().await?;
                                    Ok::<_, anyhow::Error>(())
                                });
                            }
                            join_set
                                .join_all()
                                .await
                                .into_iter()
                                .collect::<Result<Vec<_>, _>>()?;
                            return Ok(());
                        }

                        let mut previous = time::Instant::now();
                        let mut first = true;
                        let mut started_count = 0;
                        for child in &mut children {
                            if first {
                                first = false;
                            } else if !cross_wallet_transfers {
                                let time_elapsed = previous.elapsed();
                                if time_elapsed < Duration::from_secs(delay_between_processes) {
                                    time::sleep(
                                        Duration::from_secs(delay_between_processes) - time_elapsed,
                                    )
                                    .await;
                                }
                            }

                            let mut stdin: ChildStdin = child.stdin.take().unwrap();
                            stdin.write_all(b"yes\n").await?;
                            stdin.flush().await?;
                            started_count += 1;
                            info!("{}/{} benchmarks started", started_count, processes);

                            previous = time::Instant::now();
                        }
                    }

                    let shutdown_notifier = CancellationToken::new();
                    tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

                    // Start metrics server for multi-process benchmark monitoring
                    #[cfg(with_metrics)]
                    {
                        let metrics_address = std::net::SocketAddr::from(([127, 0, 0, 1], 0));
                        monitoring_server::start_metrics(
                            metrics_address,
                            shutdown_notifier.clone(),
                        );
                    }

                    let mut join_set = JoinSet::new();
                    let children_pids: Vec<u32> = children.iter().filter_map(|c| c.id()).collect();

                    for ((mut child, stdout_handle), stderr_handle) in
                        children.into_iter().zip(stdout_handles).zip(stderr_handles)
                    {
                        join_set.spawn(async move {
                            let pid = child.id();
                            let status = child.wait().await?;
                            stdout_handle.await?;
                            stderr_handle.await?;
                            Ok::<_, anyhow::Error>((pid, status))
                        });
                    }

                    loop {
                        tokio::select! {
                            result = join_set.join_next() => {
                                match result {
                                    Some(Ok(Ok((pid, status)))) => {
                                        if !status.success() {
                                            error!("Benchmark process (pid {:?}) failed with status: {:?}", pid, status);
                                            kill_all_processes(&children_pids).await;
                                            return Err(anyhow::anyhow!("Benchmark process (pid {:?}) failed", pid));
                                        }
                                    }
                                    Some(Ok(Err(e))) => {
                                        error!("Benchmark process failed: {}", e);
                                        kill_all_processes(&children_pids).await;
                                        return Err(e);
                                    }
                                    Some(Err(e)) => {
                                        error!("Benchmark process panicked: {}", e);
                                        kill_all_processes(&children_pids).await;
                                        return Err(e.into());
                                    }
                                    None => {
                                        info!("All benchmark processes have finished");
                                        break;
                                    }
                                }
                            }
                            _ = shutdown_notifier.cancelled() => {
                                info!("Shutdown signal received, waiting for all benchmark processes to finish");
                                join_set.join_all().await.into_iter().collect::<Result<Vec<_>, _>>()?;
                                break;
                            }
                        }
                    }
                }
            },

            Watch { chain_id, raw } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let mut join_set = JoinSet::new();
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).await?;
                info!("Watching for notifications for chain {:?}", chain_id);
                let (listener, _listen_handle, mut notifications) = chain_client.listen().await?;
                join_set.spawn_task(listener);
                while let Some(notification) = notifications.next().await {
                    if let Reason::NewBlock { .. } = notification.reason {
                        context.update_wallet_from_client(&chain_client).await?;
                    }
                    if raw {
                        println!("{}", serde_json::to_string(&notification)?);
                    }
                }
                info!("Notification stream ended.");
            }

            Service {
                config,
                port,
                #[cfg(with_metrics)]
                metrics_port,
                operator_application_ids,
                operators,
                controller_application_id,
                read_only,
            } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let default_chain = context.wallet().default_chain();
                let chain_id =
                    default_chain.expect("Service requires a default chain in the wallet");

                assert!(
                    operator_application_ids.is_empty() || controller_application_id.is_none(),
                    "Cannot run a static list of applications when a controller is given."
                );

                let cancellation_token = CancellationToken::new();
                tokio::spawn(listen_for_shutdown_signals(cancellation_token.clone()));

                let operators: BTreeMap<String, PathBuf> = operators.into_iter().collect();
                for (name, path) in &operators {
                    info!("Operator '{}' -> {}", name, path.display());
                }
                let operators = Arc::new(operators);

                // Start the task processor if operator applications are specified.
                if !operator_application_ids.is_empty() {
                    let chain_client = context.make_chain_client(chain_id).await?;
                    let processor = TaskProcessor::new(
                        chain_id,
                        operator_application_ids,
                        chain_client,
                        cancellation_token.clone(),
                        operators.clone(),
                        None,
                    );
                    tokio::spawn(processor.run());
                }

                let context = Arc::new(Mutex::new(context));

                let (command_sender, command_receiver) = mpsc::unbounded_channel();

                if let Some(controller_id) = controller_application_id {
                    // For the controller case, we share the context via Arc so the
                    // controller can spawn new processors for different chains.
                    let chain_client = context.lock().await.make_chain_client(chain_id).await?;
                    let controller = Controller::new(
                        chain_id,
                        controller_id,
                        context.clone(),
                        chain_client,
                        cancellation_token.clone(),
                        operators,
                        command_sender,
                    );

                    tokio::spawn(controller.run());
                }

                let service = NodeService::new(
                    config,
                    port,
                    #[cfg(with_metrics)]
                    metrics_port,
                    Some(chain_id),
                    context,
                    read_only,
                );
                service.run(cancellation_token, command_receiver).await?;
            }

            Faucet {
                chain_id,
                port,
                #[cfg(with_metrics)]
                metrics_port,
                amount,
                limit_rate_until,
                config,
                storage_path,
                max_batch_size,
            } => {
                let genesis_config = wallet.genesis_config().clone();

                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let chain_id = if let Some(chain_id) = chain_id {
                    chain_id
                } else {
                    context.first_non_admin_chain().await?
                };
                info!("Starting faucet service using chain {}", chain_id);
                let end_timestamp = limit_rate_until.map_or_else(Timestamp::now, |et| {
                    let micros =
                        u64::try_from(et.timestamp_micros()).expect("End timestamp before 1970");
                    Timestamp::from(micros)
                });
                let config = FaucetConfig {
                    port,
                    #[cfg(with_metrics)]
                    metrics_port,
                    chain_id,
                    amount,
                    end_timestamp,
                    genesis_config: Arc::new(genesis_config),
                    chain_listener_config: config,
                    storage_path,
                    max_batch_size,
                };
                let faucet = FaucetService::new(config, context).await?;
                let cancellation_token = CancellationToken::new();
                let child_token = cancellation_token.child_token();
                tokio::spawn(listen_for_shutdown_signals(cancellation_token));
                faucet.run(child_token).await?;
            }

            PublishModule {
                contract,
                service,
                vm_runtime,
                publisher,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing module on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher).await?;
                let module_id = context
                    .publish_module(&chain_client, contract, service, vm_runtime)
                    .await?;
                println!("{}", module_id);
                info!(
                    "Module published in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            ListEventsFromIndex {
                chain_id,
                stream_id,
                start_index,
            } => {
                let context = options
                    .create_client_context(storage.clone(), wallet, signer.into_value())
                    .await?;
                let start_time = Instant::now();
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let index_events = storage
                    .read_events_from_index(&chain_id, &stream_id, start_index)
                    .await?;
                println!("{:#?}", index_events);
                info!("Events listed in {} ms", start_time.elapsed().as_millis());
            }

            PublishDataBlob {
                blob_path,
                publisher,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing data blob on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher).await?;
                let hash = context.publish_data_blob(&chain_client, blob_path).await?;
                println!("{}", hash);
                info!(
                    "Data blob published in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            // TODO(#2490): Consider removing or renaming this.
            ReadDataBlob { hash, reader } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let start_time = Instant::now();
                let reader = reader.unwrap_or_else(|| context.default_chain());
                info!("Verifying data blob on chain {}", reader);
                let chain_client = context.make_chain_client(reader).await?;
                context.read_data_blob(&chain_client, hash).await?;
                info!("Data blob read in {} ms", start_time.elapsed().as_millis());
            }

            CreateApplication {
                module_id,
                creator,
                json_parameters,
                json_parameters_path,
                json_argument,
                json_argument_path,
                required_application_ids,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let start_time = Instant::now();
                let creator = creator.unwrap_or_else(|| context.default_chain());
                info!("Creating application on chain {}", creator);
                let chain_client = context.make_chain_client(creator).await?;
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                info!("Synchronizing");
                context.process_inbox(&chain_client).await?;

                let (application_id, _) = context
                    .apply_client_command(&chain_client, move |chain_client| {
                        let parameters = parameters.clone();
                        let argument = argument.clone();
                        let chain_client = chain_client.clone();
                        let required_application_ids = required_application_ids.clone();
                        async move {
                            chain_client
                                .create_application_untyped(
                                    module_id,
                                    parameters,
                                    argument,
                                    required_application_ids.unwrap_or_default(),
                                )
                                .await
                        }
                    })
                    .await
                    .context("Failed to create application")?;
                info!("{}", "Application created successfully!".green().bold());
                info!(
                    "Application created in {} ms",
                    start_time.elapsed().as_millis()
                );
                println!("{}", application_id);
            }

            PublishAndCreate {
                contract,
                service,
                vm_runtime,
                publisher,
                json_parameters,
                json_parameters_path,
                json_argument,
                json_argument_path,
                required_application_ids,
            } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;

                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing and creating application on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher).await?;
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;
                let module_id = context
                    .publish_module(&chain_client, contract, service, vm_runtime)
                    .await?;

                let (application_id, _) = context
                    .apply_client_command(&chain_client, move |chain_client| {
                        let parameters = parameters.clone();
                        let argument = argument.clone();
                        let chain_client = chain_client.clone();
                        let required_application_ids = required_application_ids.clone();
                        async move {
                            chain_client
                                .create_application_untyped(
                                    module_id,
                                    parameters,
                                    argument,
                                    required_application_ids.unwrap_or_default(),
                                )
                                .await
                        }
                    })
                    .await
                    .context("Failed to create application")?;
                info!("{}", "Application published successfully!".green().bold());
                info!(
                    "Application published and created in {} ms",
                    start_time.elapsed().as_millis()
                );
                println!("{}", application_id);
            }

            Assign { owner, chain_id } => {
                let mut context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let start_time = Instant::now();
                info!(
                    "Linking chain {chain_id} to its corresponding key in the wallet, owned by \
                    {owner}",
                );
                context.assign_new_chain_to_key(chain_id, owner).await?;
                info!(
                    "Chain linked to owner in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            Project(project_command) => match project_command {
                ProjectCommand::PublishAndCreate {
                    path,
                    name,
                    vm_runtime,
                    publisher,
                    json_parameters,
                    json_parameters_path,
                    json_argument,
                    json_argument_path,
                    required_application_ids,
                } => {
                    let mut context = options
                        .create_client_context(storage, wallet, signer.into_value())
                        .await?;
                    let start_time = Instant::now();
                    let publisher = publisher.unwrap_or_else(|| context.default_chain());
                    info!("Creating application on chain {}", publisher);
                    let chain_client = context.make_chain_client(publisher).await?;

                    let parameters = read_json(json_parameters, json_parameters_path)?;
                    let argument = read_json(json_argument, json_argument_path)?;
                    let project_path = path.unwrap_or_else(|| env::current_dir().unwrap());

                    let project = project::Project::from_existing_project(project_path)?;
                    let (contract_path, service_path) = project.build(name)?;

                    let module_id = context
                        .publish_module(&chain_client, contract_path, service_path, vm_runtime)
                        .await?;

                    let (application_id, _) = context
                        .apply_client_command(&chain_client, move |chain_client| {
                            let parameters = parameters.clone();
                            let argument = argument.clone();
                            let chain_client = chain_client.clone();
                            let required_application_ids = required_application_ids.clone();
                            async move {
                                chain_client
                                    .create_application_untyped(
                                        module_id,
                                        parameters,
                                        argument,
                                        required_application_ids.unwrap_or_default(),
                                    )
                                    .await
                            }
                        })
                        .await
                        .context("Failed to create application")?;
                    info!("{}", "Application published successfully!".green().bold());
                    info!(
                        "Project published and created in {} ms",
                        start_time.elapsed().as_millis()
                    );
                    println!("{}", application_id);
                }
                _ => unreachable!("other project commands do not require storage"),
            },

            RetryPendingBlock { chain_id } => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let start_time = Instant::now();
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                info!("Committing pending block for chain {}", chain_id);
                let chain_client = context.make_chain_client(chain_id).await?;
                match chain_client.process_pending_block().await? {
                    ClientOutcome::Committed(Some(certificate)) => {
                        info!("Pending block committed successfully.");
                        println!("{}", certificate.hash());
                    }
                    ClientOutcome::Committed(None) => info!("No block is currently pending."),
                    ClientOutcome::WaitForTimeout(timeout) => {
                        info!("Please try again at {}", timeout.timestamp)
                    }
                    ClientOutcome::Conflict(certificate) => {
                        info!("A different block was committed: {}", certificate.hash())
                    }
                }
                context.update_wallet_from_client(&chain_client).await?;
                info!(
                    "Pending block retried in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            Wallet(WalletCommand::RequestChain {
                faucet: faucet_url,
                set_default,
            }) => {
                let start_time = Instant::now();
                let owner: AccountOwner = signer.mutate(|s| s.generate_new()).await?.into();

                info!(
                    "Requesting a new chain for owner {owner} using the faucet at address \
                     {faucet_url}",
                );

                let description = cli_wrappers::Faucet::new(faucet_url).claim(&owner).await?;

                if !description.config().ownership.is_owner(&owner) {
                    anyhow::bail!(
                        "The chain with the ID returned by the faucet is not owned by you. \
                         Please make sure you are connecting to a genuine faucet."
                    );
                }

                wallet.insert(
                    description.id(),
                    wallet::Chain {
                        owner: Some(owner),
                        ..(&description).into()
                    },
                )?;

                if set_default {
                    wallet.set_default_chain(description.id())?;
                }

                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_client = context.make_chain_client(description.id()).await?;
                chain_client.synchronize_from_validators().await?;
                context.update_wallet_from_client(&chain_client).await?;

                // print this only after adding the chain to the wallet
                println!("{}", description.id());
                println!("{owner}");

                info!(
                    "New chain requested and added in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            Wallet(WalletCommand::Init {
                faucet,
                genesis_config_path,
                ..
            }) => {
                let (Some(faucet_url), None) = (faucet, genesis_config_path) else {
                    return Ok(());
                };
                let Some(network_description) = storage.read_network_description().await? else {
                    anyhow::bail!("Missing network description");
                };
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let faucet = cli_wrappers::Faucet::new(faucet_url);
                let committee = faucet.current_committee().await?;
                let chain_client = context
                    .make_chain_client(network_description.admin_chain_id)
                    .await?;
                chain_client
                    .synchronize_chain_state_from_committee(committee)
                    .await?;
                context.update_wallet_from_client(&chain_client).await?;
            }

            Wallet(WalletCommand::FollowChain { chain_id, sync }) => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let start_time = Instant::now();
                context
                    .client
                    .extend_chain_mode(chain_id, ListeningMode::FollowChain);
                let chain_client = context.make_chain_client(chain_id).await?;
                if sync {
                    chain_client.synchronize_chain_state(chain_id).await?;
                } else {
                    chain_client.fetch_chain_info().await?;
                }
                context.update_wallet_from_client(&chain_client).await?;
                // Update the in-memory state to follow-only mode.
                context.client.set_chain_follow_only(chain_id, true);
                info!(
                    "Chain followed and added in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            Chain(ChainCommand::ShowBlock { chain_id, height }) => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_state_view = context
                    .storage()
                    .load_chain(chain_id)
                    .await
                    .context("Failed to load chain")?;
                let block_hash = chain_state_view
                    .block_hashes([height])
                    .await
                    .context("Failed to find a block hash for the given height")?[0];
                let block = context
                    .storage()
                    .read_confirmed_block(block_hash)
                    .await
                    .context("Failed to find the given block in storage")?;
                println!("{:#?}", block);
            }

            Chain(ChainCommand::ShowChainDescription { chain_id }) => {
                let context = options
                    .create_client_context(storage, wallet, signer.into_value())
                    .await?;
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).await?;
                let description = match chain_client.get_chain_description().await {
                    Ok(description) => description,
                    Err(chain_client::Error::LocalNodeError(LocalNodeError::BlobsNotFound(_))) => {
                        println!("Could not find a chain description corresponding to the given chain ID.");
                        return Ok(());
                    }
                    err => err.context("Failed to get the chain description")?,
                };
                println!("{:#?}", description);
            }

            Validator(validator_command) => {
                validator_command
                    .run(
                        &mut options
                            .create_client_context(storage, wallet, signer.into_value())
                            .await?,
                    )
                    .await?;
            }

            CreateGenesisConfig { .. }
            | Keygen
            | Net(_)
            | Storage { .. }
            | Wallet(_)
            | ExtractScriptFromMarkdown { .. }
            | HelpMarkdown
            | Completion { .. } => {
                unreachable!()
            }
        }
        Ok(())
    }
}

async fn kill_all_processes(pids: &[u32]) {
    for &pid in pids {
        info!("Killing benchmark process (pid {})", pid);
        // Ignore kill errors; the process may have already exited.
        Command::new("kill")
            .arg("-9")
            .arg(pid.to_string())
            .status()
            .await
            .ok();
    }
}

struct DatabaseToolJob<'a>(&'a DatabaseToolCommand);

#[async_trait]
impl RunnableWithStore for DatabaseToolJob<'_> {
    type Output = i32;

    async fn run<D>(
        self,
        config: D::Config,
        namespace: String,
    ) -> Result<Self::Output, anyhow::Error>
    where
        D: KeyValueDatabase + Clone + Send + Sync + 'static,
        D::Store: KeyValueStore + Clone + Send + Sync + 'static,
        D::Error: Send + Sync,
    {
        let start_time = Instant::now();
        match self.0 {
            DatabaseToolCommand::DeleteAll => {
                D::delete_all(&config).await?;
                info!(
                    "All namespaces deleted in {} ms",
                    start_time.elapsed().as_millis()
                );
            }
            DatabaseToolCommand::DeleteNamespace => {
                D::delete(&config, &namespace).await?;
                info!(
                    "Namespace {namespace} deleted in {} ms",
                    start_time.elapsed().as_millis()
                );
            }
            DatabaseToolCommand::CheckExistence => {
                let test = D::exists(&config, &namespace).await?;
                info!(
                    "Existence of a namespace {namespace} checked in {} ms",
                    start_time.elapsed().as_millis()
                );
                if test {
                    info!("The namespace {namespace} does exist in storage");
                    return Ok(0);
                } else {
                    info!("The namespace {namespace} does not exist in storage");
                    return Ok(1);
                }
            }
            DatabaseToolCommand::Initialize {
                genesis_config_path,
            } => {
                let genesis_config: GenesisConfig = util::read_json(genesis_config_path)?;
                let mut storage =
                    DbStorage::<D, _>::maybe_create_and_connect(&config, &namespace, None).await?;
                genesis_config.initialize_storage(&mut storage).await?;
                info!(
                    "Namespace {namespace} was initialized in {} ms",
                    start_time.elapsed().as_millis()
                );
            }
            DatabaseToolCommand::ListNamespaces => {
                let namespaces = D::list_all(&config).await?;
                info!(
                    "Namespaces listed in {} ms",
                    start_time.elapsed().as_millis()
                );
                info!("The list of namespaces is:");
                for namespace in namespaces {
                    println!("{}", namespace);
                }
            }
            DatabaseToolCommand::ListBlobIds => {
                let storage =
                    DbStorage::<D, _>::maybe_create_and_connect(&config, &namespace, None).await?;
                let blob_ids = storage.list_blob_ids().await?;
                info!("Blob IDs listed in {} ms", start_time.elapsed().as_millis());
                info!("The list of blob IDs is:");
                for id in blob_ids {
                    println!("{}", id);
                }
            }
            DatabaseToolCommand::ListChainIds => {
                let storage =
                    DbStorage::<D, _>::maybe_create_and_connect(&config, &namespace, None).await?;
                let chain_ids = storage.list_chain_ids().await?;
                info!(
                    "Chain IDs listed in {} ms",
                    start_time.elapsed().as_millis()
                );
                info!("The list of chain IDs is:");
                for id in chain_ids {
                    println!("{}", id);
                }
            }
            DatabaseToolCommand::ListEventIds => {
                let storage =
                    DbStorage::<D, _>::maybe_create_and_connect(&config, &namespace, None).await?;
                let event_ids = storage.list_event_ids().await?;
                info!(
                    "Event IDs listed in {} ms",
                    start_time.elapsed().as_millis()
                );
                info!("The list of event IDs is:");
                for id in event_ids {
                    println!("{}", id);
                }
            }
        }
        Ok(0)
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn init_tracing(
    options: &Options,
) -> anyhow::Result<Option<linera_service::tracing::chrome::ChromeTraceGuard>> {
    if matches!(&options.command, ClientCommand::Faucet { .. }) {
        linera_service::tracing::opentelemetry::init(
            &options.command.log_file_name(),
            options.otlp_exporter_endpoint.as_deref(),
        );
        Ok(None)
    } else if options.chrome_trace_exporter {
        let trace_file_path = options.chrome_trace_file.as_deref().map_or_else(
            || format!("{}.trace.json", options.command.log_file_name()),
            |s| s.to_string(),
        );
        let writer = std::fs::File::create(&trace_file_path)?;
        Ok(Some(linera_service::tracing::chrome::init(
            &options.command.log_file_name(),
            writer,
        )))
    } else {
        linera_service::tracing::init(&options.command.log_file_name());
        Ok(None)
    }
}

#[cfg(target_arch = "wasm32")]
fn init_tracing(options: &Options) {
    linera_base::tracing::init(&options.command.log_file_name());
}

fn main() -> anyhow::Result<process::ExitCode> {
    let options = Options::init();
    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    // The default stack size 2 MiB causes some stack overflows in ValidatorUpdater methods.
    runtime.thread_stack_size(4 << 20);
    if let Some(blocking_threads) = options.tokio_blocking_threads {
        runtime.max_blocking_threads(blocking_threads);
    }

    let span = tracing::info_span!("linera::main");
    if let Some(wallet_id) = &options.with_wallet {
        span.record("wallet_id", wallet_id);
    }

    let result = runtime
        .enable_all()
        .build()?
        .block_on(run(&options).instrument(span));

    Ok(match result {
        Ok(0) => process::ExitCode::SUCCESS,
        Ok(code) => process::ExitCode::from(code as u8),
        Err(msg) => {
            error!("Error is {:?}", msg);
            process::ExitCode::FAILURE
        }
    })
}

async fn run(options: &Options) -> Result<i32, Error> {
    let _guard = init_tracing(options)?;
    match &options.command {
        ClientCommand::HelpMarkdown => {
            clap_markdown::print_help_markdown::<Options>();
            Ok(0)
        }

        ClientCommand::ExtractScriptFromMarkdown {
            path,
            pause_after_linera_service,
            pause_after_gql_mutations,
        } => {
            let file = crate::util::Markdown::new(path)?;
            let pause_after_linera_service =
                Some(*pause_after_linera_service).filter(|p| !p.is_zero());
            let pause_after_gql_mutations =
                Some(*pause_after_gql_mutations).filter(|p| !p.is_zero());
            file.extract_bash_script_to(
                std::io::stdout(),
                pause_after_linera_service,
                pause_after_gql_mutations,
            )?;
            Ok(0)
        }

        ClientCommand::Completion { shell } => {
            let mut cmd = <Options as clap::CommandFactory>::command();
            generate(
                *shell,
                &mut cmd,
                env!("CARGO_BIN_NAME"),
                &mut std::io::stdout(),
            );
            Ok(0)
        }

        ClientCommand::CreateGenesisConfig {
            committee_config_path,
            genesis_config_path,
            initial_funding,
            start_timestamp,
            num_other_initial_chains,
            policy_config,
            wasm_fuel_unit_price,
            evm_fuel_unit_price,
            read_operation_price,
            write_operation_price,
            byte_runtime_price,
            byte_read_price,
            byte_written_price,
            byte_stored_price,
            blob_read_price,
            blob_published_price,
            blob_byte_read_price,
            blob_byte_published_price,
            operation_price,
            operation_byte_price,
            message_price,
            message_byte_price,
            service_as_oracle_query_price,
            http_request_price,
            maximum_wasm_fuel_per_block,
            maximum_evm_fuel_per_block,
            maximum_service_oracle_execution_ms,
            maximum_block_size,
            maximum_blob_size,
            maximum_published_blobs,
            maximum_bytecode_size,
            maximum_block_proposal_size,
            maximum_bytes_read_per_block,
            maximum_bytes_written_per_block,
            maximum_oracle_response_bytes,
            maximum_http_response_bytes,
            http_request_timeout_ms,
            http_request_allow_list,
            testing_prng_seed,
            network_name,
        } => {
            let start_time = Instant::now();
            let committee_config: CommitteeConfig = util::read_json(committee_config_path)
                .expect("Unable to read committee config file");
            let existing_policy = policy_config.into_policy();
            let policy = linera_execution::ResourceControlPolicy {
                wasm_fuel_unit: wasm_fuel_unit_price.unwrap_or(existing_policy.wasm_fuel_unit),
                evm_fuel_unit: evm_fuel_unit_price.unwrap_or(existing_policy.evm_fuel_unit),
                read_operation: read_operation_price.unwrap_or(existing_policy.read_operation),
                write_operation: write_operation_price.unwrap_or(existing_policy.write_operation),
                byte_runtime: byte_runtime_price.unwrap_or(existing_policy.byte_runtime),
                byte_read: byte_read_price.unwrap_or(existing_policy.byte_read),
                byte_written: byte_written_price.unwrap_or(existing_policy.byte_written),
                blob_read: blob_read_price.unwrap_or(existing_policy.blob_read),
                blob_published: blob_published_price.unwrap_or(existing_policy.blob_published),
                blob_byte_read: blob_byte_read_price.unwrap_or(existing_policy.blob_byte_read),
                blob_byte_published: blob_byte_published_price
                    .unwrap_or(existing_policy.blob_byte_published),
                byte_stored: byte_stored_price.unwrap_or(existing_policy.byte_stored),
                operation: operation_price.unwrap_or(existing_policy.operation),
                operation_byte: operation_byte_price.unwrap_or(existing_policy.operation_byte),
                message: message_price.unwrap_or(existing_policy.message),
                message_byte: message_byte_price.unwrap_or(existing_policy.message_byte),
                service_as_oracle_query: service_as_oracle_query_price
                    .unwrap_or(existing_policy.service_as_oracle_query),
                http_request: http_request_price.unwrap_or(existing_policy.http_request),
                maximum_wasm_fuel_per_block: maximum_wasm_fuel_per_block
                    .unwrap_or(existing_policy.maximum_wasm_fuel_per_block),
                maximum_evm_fuel_per_block: maximum_evm_fuel_per_block
                    .unwrap_or(existing_policy.maximum_evm_fuel_per_block),
                maximum_service_oracle_execution_ms: maximum_service_oracle_execution_ms
                    .unwrap_or(existing_policy.maximum_service_oracle_execution_ms),
                maximum_block_size: maximum_block_size
                    .unwrap_or(existing_policy.maximum_block_size),
                maximum_bytecode_size: maximum_bytecode_size
                    .unwrap_or(existing_policy.maximum_bytecode_size),
                maximum_blob_size: maximum_blob_size.unwrap_or(existing_policy.maximum_blob_size),
                maximum_published_blobs: maximum_published_blobs
                    .unwrap_or(existing_policy.maximum_published_blobs),
                maximum_block_proposal_size: maximum_block_proposal_size
                    .unwrap_or(existing_policy.maximum_block_proposal_size),
                maximum_bytes_read_per_block: maximum_bytes_read_per_block
                    .unwrap_or(existing_policy.maximum_bytes_read_per_block),
                maximum_bytes_written_per_block: maximum_bytes_written_per_block
                    .unwrap_or(existing_policy.maximum_bytes_written_per_block),
                maximum_oracle_response_bytes: maximum_oracle_response_bytes
                    .unwrap_or(existing_policy.maximum_oracle_response_bytes),
                maximum_http_response_bytes: maximum_http_response_bytes
                    .unwrap_or(existing_policy.maximum_http_response_bytes),
                http_request_timeout_ms: http_request_timeout_ms
                    .unwrap_or(existing_policy.http_request_timeout_ms),
                http_request_allow_list: http_request_allow_list
                    .as_ref()
                    .map(|list| list.iter().cloned().collect())
                    .unwrap_or(existing_policy.http_request_allow_list),
            };
            let timestamp = start_timestamp.map_or_else(Timestamp::now, |st| {
                let micros =
                    u64::try_from(st.timestamp_micros()).expect("Start timestamp before 1970");
                Timestamp::from(micros)
            });

            let mut signer = options.create_keystore(*testing_prng_seed)?;
            let admin_public_key = signer.mutate(|s| s.generate_new()).await?;

            let network_name = network_name.clone().unwrap_or_else(|| {
                // Default: e.g. "linera-2023-11-14T23:13:20"
                format!("linera-{}", Utc::now().naive_utc().format("%FT%T"))
            });
            let mut genesis_config = persistent::File::new(
                genesis_config_path,
                GenesisConfig::new(
                    committee_config,
                    timestamp,
                    policy,
                    network_name,
                    admin_public_key,
                    *initial_funding,
                ),
            )?;
            let admin_chain_description = genesis_config.admin_chain_description();
            let mut chains = vec![(
                admin_chain_description.id(),
                wallet::Chain {
                    owner: Some(admin_public_key.into()),
                    epoch: Some(admin_chain_description.config().epoch),
                    timestamp,
                    ..wallet::Chain::default()
                },
            )];
            for _ in 0..*num_other_initial_chains {
                // Create keys.
                let public_key = signer.mutate(|s| s.generate_new()).await?;
                let description = genesis_config.add_root_chain(public_key, *initial_funding);
                let chain = wallet::Chain {
                    owner: Some(public_key.into()),
                    epoch: Some(description.config().epoch),
                    timestamp,
                    ..wallet::Chain::default()
                };
                chains.push((description.id(), chain));
            }
            genesis_config.persist().await?;
            let mut wallet = options.create_wallet(genesis_config.into_value())?;
            wallet.extend(chains);
            wallet.save()?;
            options.initialize_storage().boxed().await?;
            info!(
                "Genesis config created in {} ms",
                start_time.elapsed().as_millis()
            );
            Ok(0)
        }

        ClientCommand::Project(project_command) => match project_command {
            ProjectCommand::New { name, linera_root } => {
                let start_time = Instant::now();
                Project::create_new(name, linera_root.as_ref().map(AsRef::as_ref))?;
                info!(
                    "New project created in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }
            ProjectCommand::Test { path } => {
                let start_time = Instant::now();
                let path = path.clone().unwrap_or_else(|| env::current_dir().unwrap());
                let project = Project::from_existing_project(path)?;
                project.test()?;
                info!(
                    "Test project created in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }
            ProjectCommand::PublishAndCreate { .. } => {
                let start_time = Instant::now();
                options.run_with_storage(Job(options.clone())).await??;
                info!(
                    "Project published and created in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }
        },

        ClientCommand::Keygen => {
            let start_time = Instant::now();
            let mut signer = options.signer()?;
            let public_key = signer.mutate(|s| s.generate_new()).await?;
            let owner = AccountOwner::from(public_key);
            println!("{}", owner);
            info!("Key generated in {} ms", start_time.elapsed().as_millis());
            Ok(0)
        }

        ClientCommand::Net(net_command) => match net_command {
            #[cfg(feature = "kubernetes")]
            NetCommand::Up {
                other_initial_chains,
                initial_amount,
                validators,
                proxies,
                shards,
                testing_prng_seed,
                policy_config,
                kubernetes: true,
                binaries,
                no_build,
                docker_image_name,
                build_mode,
                with_faucet,
                faucet_chain,
                faucet_port,
                faucet_amount,
                with_block_exporter,
                num_block_exporters,
                indexer_image_name,
                explorer_image_name,
                dual_store,
                path,
                ..
            } => {
                net_up_utils::handle_net_up_kubernetes(
                    *other_initial_chains,
                    *initial_amount,
                    *validators,
                    *proxies,
                    *shards,
                    *testing_prng_seed,
                    binaries,
                    *no_build,
                    docker_image_name.clone(),
                    build_mode.clone(),
                    *policy_config,
                    *with_faucet,
                    *faucet_chain,
                    *faucet_port,
                    *faucet_amount,
                    *with_block_exporter,
                    *num_block_exporters,
                    indexer_image_name.clone(),
                    explorer_image_name.clone(),
                    *dual_store,
                    path,
                )
                .boxed()
                .await?;
                Ok(0)
            }

            NetCommand::Up {
                other_initial_chains,
                initial_amount,
                validators,
                shards,
                testing_prng_seed,
                policy_config,
                cross_chain_config,
                path,
                external_protocol,
                with_faucet,
                faucet_chain,
                faucet_port,
                faucet_amount,
                with_block_exporter,
                exporter_address: block_exporter_address,
                exporter_port: block_exporter_port,
                ..
            } => {
                net_up_utils::handle_net_up_service(
                    *other_initial_chains,
                    *initial_amount,
                    *validators,
                    *shards,
                    *testing_prng_seed,
                    *policy_config,
                    cross_chain_config.clone(),
                    *with_block_exporter,
                    block_exporter_address.to_owned(),
                    *block_exporter_port,
                    path,
                    // Not using the default value for storage
                    &options.storage_config,
                    external_protocol.clone(),
                    *with_faucet,
                    *faucet_chain,
                    *faucet_port,
                    *faucet_amount,
                )
                .boxed()
                .await?;
                Ok(0)
            }

            NetCommand::Helper => {
                info!("You may append the following script to your `~/.bash_profile` or `source` it when needed.");
                info!(
                    "This will install several functions to facilitate \
                       testing with a local Linera network"
                );
                println!("{}", include_str!("../../template/linera_net_helper.sh"));
                Ok(0)
            }
        },

        ClientCommand::Storage(command) => {
            Ok(options.run_with_store(DatabaseToolJob(command)).await?)
        }

        ClientCommand::Wallet(wallet_command) => match wallet_command {
            WalletCommand::Show {
                chain_id,
                short,
                owned,
            } => {
                let wallet_path = options.wallet_path()?;
                tracing::info!("Reading wallet from file: {}", wallet_path.display());
                let wallet = options.wallet()?;
                let chain_ids = if let Some(chain_id) = chain_id {
                    ensure!(!owned, "Cannot specify both --owned and a chain ID");
                    vec![*chain_id]
                } else if *owned {
                    wallet.owned_chain_ids()
                } else {
                    wallet.chain_ids()
                };
                if *short {
                    for chain_id in chain_ids {
                        println!("{chain_id}");
                    }
                } else {
                    wallet.pretty_print(chain_ids);
                }
                Ok(0)
            }

            WalletCommand::SetDefault { chain_id } => {
                let start_time = Instant::now();
                options.wallet()?.set_default_chain(*chain_id)?;
                info!(
                    "Default chain set in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }

            WalletCommand::ForgetKeys { chain_id } => {
                let start_time = Instant::now();
                let owner = options.wallet()?.forget_keys(*chain_id)?;
                if !options
                    .signer()?
                    .contains_key(&owner)
                    .await
                    .expect("Signer error")
                {
                    warn!("no keypair found in keystore for chain {chain_id}");
                }
                info!(
                    "Chain keys forgotten in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }

            WalletCommand::ForgetChain { chain_id } => {
                let start_time = Instant::now();
                options.wallet()?.forget_chain(*chain_id)?;
                info!("Chain forgotten in {} ms", start_time.elapsed().as_millis());
                Ok(0)
            }

            WalletCommand::Init {
                genesis_config_path,
                faucet,
                testing_prng_seed,
            } => {
                let start_time = Instant::now();
                let genesis_config: GenesisConfig = match (genesis_config_path, faucet) {
                    (None, None) => {
                        anyhow::bail!("please specify one of `--faucet` or `--genesis`.")
                    }
                    (Some(genesis_config_path), _) => util::read_json(genesis_config_path)?,
                    (None, Some(url)) => {
                        let faucet = cli_wrappers::Faucet::new(url.clone());
                        let version_info = faucet
                            .version_info()
                            .await
                            .context("Failed to obtain version information from the faucet")?;
                        if !version_info.is_compatible_with(&linera_version::VERSION_INFO) {
                            warn!(
                                "\
Make sure to use a Linera client compatible with this network.
--- Faucet info ---\
{}\
-------------------
--- This binary ---\
{}\
-------------------",
                                version_info,
                                linera_version::VERSION_INFO,
                            );
                        }
                        faucet
                            .genesis_config()
                            .await
                            .context("Failed to obtain the genesis configuration from the faucet")?
                    }
                };
                let mut keystore = options.create_keystore(*testing_prng_seed)?;
                keystore.persist().await?;
                options.create_wallet(genesis_config)?.save()?;
                options.initialize_storage().boxed().await?;
                options.run_with_storage(Job(options.clone())).await??;
                info!(
                    "Wallet initialized in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }

            WalletCommand::ExportGenesis { output, faucet } => {
                let genesis_config: GenesisConfig = if let Some(url) = faucet {
                    let faucet = cli_wrappers::Faucet::new(url.clone());
                    faucet
                        .genesis_config()
                        .await
                        .context("Failed to obtain the genesis configuration from the faucet")?
                } else {
                    let wallet = options.wallet()?;
                    wallet.genesis_config().clone()
                };
                let json = serde_json::to_string_pretty(&genesis_config)?;
                std::fs::write(output, json)
                    .context("Failed to write genesis configuration to file")?;
                info!("Genesis configuration exported to {}", output.display());
                Ok(0)
            }

            WalletCommand::FollowChain { .. } | WalletCommand::RequestChain { .. } => {
                options.run_with_storage(Job(options.clone())).await??;
                Ok(0)
            }
        },

        ClientCommand::Validator(_) => {
            options.run_with_storage(Job(options.clone())).await??;
            Ok(0)
        }

        _ => {
            options.run_with_storage(Job(options.clone())).await??;
            Ok(0)
        }
    }
}
