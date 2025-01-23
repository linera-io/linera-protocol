// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![recursion_limit = "256"]
#![deny(clippy::large_futures)]

use std::{
    borrow::Cow, collections::HashMap, env, num::NonZeroUsize, path::PathBuf, process, sync::Arc,
    time::Instant,
};

use anyhow::{anyhow, bail, ensure, Context};
use async_trait::async_trait;
use chrono::Utc;
use colored::Colorize;
use futures::{lock::Mutex, FutureExt as _, StreamExt};
use linera_base::{
    crypto::{CryptoHash, CryptoRng},
    data_types::{ApplicationPermissions, Timestamp},
    identifiers::{AccountOwner, ChainDescription, ChainId, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_client::{
    chain_listener::ClientContext as _,
    client_context::ClientContext,
    client_options::{
        ClientCommand, ClientOptions, DatabaseToolCommand, NetCommand, ProjectCommand,
        WalletCommand,
    },
    config::{CommitteeConfig, GenesisConfig},
    persistent::{self, Persist},
    storage::Runnable,
    wallet::{UserChain, Wallet},
};
use linera_core::{
    client,
    data_types::{ChainInfoQuery, ClientOutcome},
    node::{CrossChainMessageDelivery, ValidatorNodeProvider},
    remote_node::RemoteNode,
    worker::Reason,
    JoinSetExt as _, DEFAULT_GRACE_PERIOD,
};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    Message, ResourceControlPolicy, SystemMessage,
};
use linera_service::{
    cli_wrappers,
    faucet::FaucetService,
    node_service::NodeService,
    project::{self, Project},
    util, wallet,
};
use linera_storage::Storage;
use linera_views::store::CommonStoreConfig;
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn, Instrument as _};

mod net_up_utils;

#[cfg(feature = "benchmark")]
use {
    linera_base::hashed::Hashed,
    linera_chain::types::ConfirmedBlock,
    linera_core::data_types::ChainInfoResponse,
    linera_rpc::{HandleConfirmedCertificateRequest, RpcMessage},
    std::collections::HashSet,
};

use crate::persistent::PersistExt as _;

#[cfg(feature = "benchmark")]
fn deserialize_response(response: RpcMessage) -> Option<ChainInfoResponse> {
    match response {
        RpcMessage::ChainInfoResponse(info) => Some(*info),
        RpcMessage::Error(error) => {
            error!("Received error value: {}", error);
            None
        }
        _ => {
            error!("Unexpected return value");
            None
        }
    }
}

struct Job(ClientOptions);

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
        let wallet = options.wallet().await?;
        let mut context = ClientContext::new(storage.clone(), options.clone(), wallet);
        let command = options.command;

        use ClientCommand::*;
        match command {
            Transfer {
                sender,
                recipient,
                amount,
            } => {
                let chain_client = context.make_chain_client(sender.chain_id)?;
                let owner = match sender.owner {
                    Some(AccountOwner::User(owner)) => Some(owner),
                    Some(AccountOwner::Application(_)) => {
                        bail!("Can't transfer from an application account")
                    }
                    None => None,
                };
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
                                .transfer_to_account(owner, amount, recipient)
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
            } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
                let (new_owner, key_pair) = match owner {
                    Some(owner) => (owner, None),
                    None => {
                        let key_pair = context.wallet.generate_key_pair();
                        (key_pair.public().into(), Some(key_pair))
                    }
                };
                info!("Opening a new chain from existing chain {}", chain_id);
                let time_start = Instant::now();
                let (message_id, certificate) = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let ownership = ChainOwnership::single(new_owner);
                        let chain_client = chain_client.clone();
                        async move {
                            chain_client
                                .open_chain(ownership, ApplicationPermissions::default(), balance)
                                .await
                        }
                    })
                    .await
                    .context("Failed to open chain")?;
                let id = ChainId::child(message_id);
                let timestamp = certificate.block().header.timestamp;
                context
                    .update_wallet_for_new_chain(id, key_pair, timestamp)
                    .await?;
                let time_total = time_start.elapsed();
                info!(
                    "Opening a new chain confirmed after {} ms",
                    time_total.as_millis()
                );
                debug!("{:?}", certificate);
                // Print the new chain ID and message ID on stdout for scripting purposes.
                println!("{}", message_id);
                println!("{}", ChainId::child(message_id));
            }

            OpenMultiOwnerChain {
                chain_id,
                balance,
                ownership_config,
                application_permissions_config,
            } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
                info!(
                    "Opening a new multi-owner chain from existing chain {}",
                    chain_id
                );
                let time_start = Instant::now();
                let ownership = ChainOwnership::try_from(ownership_config)?;
                let application_permissions =
                    ApplicationPermissions::from(application_permissions_config);
                let (message_id, certificate) = context
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
                // No key pair. This chain can be assigned explicitly using the assign command.
                let key_pair = None;
                let id = ChainId::child(message_id);
                let timestamp = certificate.block().header.timestamp;
                context
                    .update_wallet_for_new_chain(id, key_pair, timestamp)
                    .await?;
                let time_total = time_start.elapsed();
                info!(
                    "Opening a new multi-owner chain confirmed after {} ms",
                    time_total.as_millis()
                );
                debug!("{:?}", certificate);
                // Print the new chain ID and message ID on stdout for scripting purposes.
                println!("{}", message_id);
                println!("{}", ChainId::child(message_id));
            }

            ChangeOwnership {
                chain_id,
                ownership_config,
            } => context.change_ownership(chain_id, ownership_config).await?,

            ChangeApplicationPermissions {
                chain_id,
                application_permissions_config,
            } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
                info!("Changing application permissions for chain {}", chain_id);
                let time_start = Instant::now();
                let application_permissions =
                    ApplicationPermissions::from(application_permissions_config);
                let certificate = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let application_permissions = application_permissions.clone();
                        let chain_client = chain_client.clone();
                        async move {
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
                let chain_client = context.make_chain_client(chain_id)?;
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
                        tracing::info!("Chain is already closed; nothing to do.");
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

            LocalBalance { account } => {
                let account = account.unwrap_or_else(|| context.default_account());
                let chain_client = context.make_chain_client(account.chain_id)?;
                info!("Reading the balance of {} from the local state", account);
                let time_start = Instant::now();
                let balance = match account.owner {
                    Some(owner) => chain_client.local_owner_balance(owner).await?,
                    None => chain_client.local_balance().await?,
                };
                let time_total = time_start.elapsed();
                info!("Local balance obtained after {} ms", time_total.as_millis());
                println!("{}", balance);
            }

            QueryBalance { account } => {
                let account = account.unwrap_or_else(|| context.default_account());
                let chain_client = context.make_chain_client(account.chain_id)?;
                info!(
                    "Evaluating the local balance of {account} by staging execution of known \
                    incoming messages"
                );
                let time_start = Instant::now();
                let balance = match account.owner {
                    Some(owner) => chain_client.query_owner_balance(owner).await?,
                    None => chain_client.query_balance().await?,
                };
                let time_total = time_start.elapsed();
                info!("Balance obtained after {} ms", time_total.as_millis());
                println!("{}", balance);
            }

            SyncBalance { account } => {
                let account = account.unwrap_or_else(|| context.default_account());
                let chain_client = context.make_chain_client(account.chain_id)?;
                info!("Synchronizing chain information and querying the local balance");
                warn!("This command is deprecated. Use `linera sync && linera query-balance` instead.");
                let time_start = Instant::now();
                chain_client.synchronize_from_validators().await?;
                let result = match account.owner {
                    Some(owner) => chain_client.query_owner_balance(owner).await,
                    None => chain_client.query_balance().await,
                };
                context.update_and_save_wallet(&chain_client).await?;
                let balance = result.context("Failed to synchronize from validators")?;
                let time_total = time_start.elapsed();
                info!(
                    "Synchronizing balance confirmed after {} ms",
                    time_total.as_millis()
                );
                println!("{}", balance);
            }

            Sync { chain_id } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
                info!("Synchronizing chain information");
                let time_start = Instant::now();
                chain_client.synchronize_from_validators().await?;
                context.update_and_save_wallet(&chain_client).await?;
                let time_total = time_start.elapsed();
                info!(
                    "Synchronized chain information in {} ms",
                    time_total.as_millis()
                );
            }

            ProcessInbox { chain_id } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
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

            QueryValidator {
                address,
                chain_id,
                name,
            } => {
                use linera_core::node::ValidatorNode as _;

                let node = context.make_node_provider().make_node(&address)?;
                match node.get_version_info().await {
                    Ok(version_info)
                        if version_info.is_compatible_with(&linera_version::VERSION_INFO) =>
                    {
                        info!(
                            "Version information for validator {address}: {}",
                            version_info
                        );
                    }
                    Ok(version_info) => error!(
                        "Validator version {} is not compatible with local version {}.",
                        version_info,
                        linera_version::VERSION_INFO
                    ),
                    Err(error) => {
                        error!(
                            "Failed to get version information for validator {address}:\n{error}"
                        )
                    }
                }

                let genesis_config_hash = context.wallet().genesis_config().hash();
                match node.get_genesis_config_hash().await {
                    Ok(hash) if hash == genesis_config_hash => {}
                    Ok(hash) => error!(
                        "Validator's genesis config hash {} does not match our own: {}.",
                        hash, genesis_config_hash
                    ),
                    Err(error) => {
                        error!(
                            "Failed to get genesis config hash for validator {address}:\n{error}"
                        )
                    }
                }

                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let query = linera_core::data_types::ChainInfoQuery::new(chain_id);
                match node.handle_chain_info_query(query).await {
                    Ok(response) => {
                        info!(
                            "Validator {address} sees chain {chain_id} at block height {} and epoch {:?}",
                            response.info.next_block_height,
                            response.info.epoch,
                        );
                        if let Some(name) = name {
                            if response.check(&name).is_ok() {
                                info!("Signature for public key {name} is OK.");
                            } else {
                                error!("Signature for public key {name} is NOT OK.");
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to get chain info for validator {address} and chain {chain_id}:\n{e}");
                    }
                }

                println!("{}", genesis_config_hash);
            }

            QueryValidators { chain_id } => {
                use linera_core::node::ValidatorNode as _;

                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
                info!("Querying validators about chain {}", chain_id);
                let result = chain_client.local_committee().await;
                context.update_and_save_wallet(&chain_client).await?;
                let committee = result.context("Failed to get local committee")?;
                info!(
                    "Using the local set of validators: {:?}",
                    committee.validators()
                );
                let node_provider = context.make_node_provider();
                let mut num_ok_validators = 0;
                let mut faulty_validators = vec![];
                for (name, state) in committee.validators() {
                    let address = &state.network_address;
                    let node = node_provider.make_node(address)?;
                    match node.get_version_info().await {
                        Ok(version_info) => {
                            info!(
                                "Version information for validator {name:?} at {address}:{}",
                                version_info
                            );
                        }
                        Err(e) => {
                            error!("Failed to get version information for validator {name:?} at {address}:\n{e}");
                            faulty_validators.push((
                                name,
                                address,
                                "Failed to get version information.".to_string(),
                            ));
                            continue;
                        }
                    }
                    let query = linera_core::data_types::ChainInfoQuery::new(chain_id);
                    match node.handle_chain_info_query(query).await {
                        Ok(response) => {
                            info!(
                                "Validator {name:?} at {address} sees chain {chain_id} at block height {} and epoch {:?}",
                                response.info.next_block_height,
                                response.info.epoch,
                            );
                            if response.check(name).is_ok() {
                                info!("Signature for public key {name} is OK.");
                                num_ok_validators += 1;
                            } else {
                                error!("Signature for public key {name} is NOT OK.");
                                faulty_validators.push((name, address, format!("{:?}", response)));
                            }
                        }
                        Err(e) => {
                            error!("Failed to get chain info for validator {name:?} at {address} and chain {chain_id}:\n{e}");
                            faulty_validators.push((
                                name,
                                address,
                                "Failed to get chain info.".to_string(),
                            ));
                            continue;
                        }
                    }
                }
                if !faulty_validators.is_empty() {
                    println!("{:#?}", faulty_validators);
                }
                println!("{}/{} OK.", num_ok_validators, committee.validators().len());
            }

            command @ (SetValidator { .. }
            | RemoveValidator { .. }
            | ResourceControlPolicy { .. }) => {
                use linera_core::node::ValidatorNode as _;

                info!("Starting operations to change validator set");
                let time_start = Instant::now();

                let context = Arc::new(Mutex::new(context));
                let mut context = context.lock().await;
                if let SetValidator {
                    name,
                    address,
                    votes: _,
                    skip_online_check: false,
                } = &command
                {
                    let node = context.make_node_provider().make_node(address)?;
                    match node.get_version_info().await {
                        Ok(version_info)
                            if version_info.is_compatible_with(&linera_version::VERSION_INFO) => {}
                        Ok(version_info) => bail!(
                            "Validator version {} is not compatible with local version {}.",
                            version_info,
                            linera_version::VERSION_INFO
                        ),
                        Err(error) => bail!(
                            "Failed to get version information for validator {name:?} at {address}:\n{error}"
                        ),
                    }
                    let genesis_config_hash = context.wallet().genesis_config().hash();
                    match node.get_genesis_config_hash().await {
                        Ok(hash) if hash == genesis_config_hash => {}
                        Ok(hash) => bail!(
                            "Validator's genesis config hash {} does not match our own: {}.",
                            hash,
                            genesis_config_hash
                        ),
                        Err(error) => bail!(
                            "Failed to get genesis config hash for validator {name:?} at {address}:\n{error}"
                        ),
                    }
                }
                let chain_client =
                    context.make_chain_client(context.wallet.genesis_admin_chain())?;
                let n = context
                    .process_inbox(&chain_client)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|c| c.block().messages().len())
                    .sum::<usize>();
                info!("Subscribed {} chains to new committees", n);
                let maybe_certificate = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        let command = command.clone();
                        async move {
                            // Create the new committee.
                            let mut committee = chain_client.local_committee().await.unwrap();
                            let mut policy = committee.policy().clone();
                            let mut validators = committee.validators().clone();
                            match command {
                                SetValidator {
                                    name,
                                    address,
                                    votes,
                                    skip_online_check: _,
                                } => {
                                    validators.insert(
                                        name,
                                        ValidatorState {
                                            network_address: address,
                                            votes,
                                        },
                                    );
                                }
                                RemoveValidator { name } => {
                                    if validators.remove(&name).is_none() {
                                        warn!("Skipping removal of nonexistent validator");
                                        return Ok(ClientOutcome::Committed(None));
                                    }
                                }
                                ResourceControlPolicy {
                                    block,
                                    fuel_unit,
                                    read_operation,
                                    write_operation,
                                    byte_read,
                                    byte_written,
                                    byte_stored,
                                    operation,
                                    operation_byte,
                                    message,
                                    message_byte,
                                    maximum_fuel_per_block,
                                    maximum_executed_block_size,
                                    maximum_blob_size,
                                    maximum_bytecode_size,
                                    maximum_block_proposal_size,
                                    maximum_bytes_read_per_block,
                                    maximum_bytes_written_per_block,
                                } => {
                                    if let Some(block) = block {
                                        policy.block = block;
                                    }
                                    if let Some(fuel_unit) = fuel_unit {
                                        policy.fuel_unit = fuel_unit;
                                    }
                                    if let Some(read_operation) = read_operation {
                                        policy.read_operation = read_operation;
                                    }
                                    if let Some(write_operation) = write_operation {
                                        policy.write_operation = write_operation;
                                    }
                                    if let Some(byte_read) = byte_read {
                                        policy.byte_read = byte_read;
                                    }
                                    if let Some(byte_written) = byte_written {
                                        policy.byte_written = byte_written;
                                    }
                                    if let Some(byte_stored) = byte_stored {
                                        policy.byte_stored = byte_stored;
                                    }
                                    if let Some(operation) = operation {
                                        policy.operation = operation;
                                    }
                                    if let Some(operation_byte) = operation_byte {
                                        policy.operation_byte = operation_byte;
                                    }
                                    if let Some(message) = message {
                                        policy.message = message;
                                    }
                                    if let Some(message_byte) = message_byte {
                                        policy.message_byte = message_byte;
                                    }
                                    if let Some(maximum_fuel_per_block) = maximum_fuel_per_block {
                                        policy.maximum_fuel_per_block = maximum_fuel_per_block;
                                    }
                                    if let Some(maximum_executed_block_size) =
                                        maximum_executed_block_size
                                    {
                                        policy.maximum_executed_block_size =
                                            maximum_executed_block_size;
                                    }
                                    if let Some(maximum_bytecode_size) = maximum_bytecode_size {
                                        policy.maximum_bytecode_size = maximum_bytecode_size;
                                    }
                                    if let Some(maximum_blob_size) = maximum_blob_size {
                                        policy.maximum_blob_size = maximum_blob_size;
                                    }
                                    if let Some(maximum_block_proposal_size) =
                                        maximum_block_proposal_size
                                    {
                                        policy.maximum_block_proposal_size =
                                            maximum_block_proposal_size;
                                    }
                                    if let Some(maximum_bytes_read_per_block) =
                                        maximum_bytes_read_per_block
                                    {
                                        policy.maximum_bytes_read_per_block =
                                            maximum_bytes_read_per_block;
                                    }
                                    if let Some(maximum_bytes_written_per_block) =
                                        maximum_bytes_written_per_block
                                    {
                                        policy.maximum_bytes_written_per_block =
                                            maximum_bytes_written_per_block;
                                    }
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

            FinalizeCommittee => {
                info!("Starting operations to remove old committees");
                let time_start = Instant::now();

                let chain_client =
                    context.make_chain_client(context.wallet.genesis_admin_chain())?;

                // Remove the old committee.
                info!("Finalizing current committee");
                context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        async move { chain_client.finalize_committee().await }
                    })
                    .await
                    .context("Failed to finalize committee")?;
                context.save_wallet().await?;

                let time_total = time_start.elapsed();
                info!(
                    "Finalizing committee confirmed after {} ms",
                    time_total.as_millis()
                );
            }

            #[cfg(feature = "benchmark")]
            Benchmark {
                max_in_flight,
                num_chains,
                tokens_per_chain,
                transactions_per_block,
                fungible_application_id,
            } => {
                // Below all block proposals are supposed to succeed without retries, we
                // must make sure that all incoming payments have been accepted on-chain
                // and that no validator is missing user certificates.
                context.process_inboxes_and_force_validator_updates().await;

                let key_pairs = context
                    .make_benchmark_chains(num_chains, tokens_per_chain)
                    .await?;

                if let Some(id) = fungible_application_id {
                    context
                        .supply_fungible_tokens(&key_pairs, id, max_in_flight)
                        .await?;
                }

                // For this command, we create proposals and gather certificates without using
                // the client library. We update the wallet storage at the end using a local node.
                info!("Starting benchmark phase 1 (block proposals)");
                let proposals = context.make_benchmark_block_proposals(
                    &key_pairs,
                    transactions_per_block,
                    fungible_application_id,
                );
                let num_proposal = proposals.len();
                let mut values = HashMap::new();

                for rpc_msg in &proposals {
                    if let RpcMessage::BlockProposal(proposal) = rpc_msg {
                        let executed_block = context
                            .stage_block_execution(proposal.content.block.clone())
                            .await?;
                        let value = Hashed::new(ConfirmedBlock::new(executed_block));
                        values.insert(value.hash(), value);
                    }
                }

                let responses = context
                    .mass_broadcast("block proposals", max_in_flight, proposals)
                    .await;
                let votes = responses
                    .into_iter()
                    .filter_map(|message| {
                        let response = deserialize_response(message)?;
                        let vote = response.info.manager.pending?;
                        let value = values.get(&vote.value.value_hash)?.clone();
                        vote.clone().with_value(value)
                    })
                    .collect::<Vec<_>>();
                info!("Received {} valid votes.", votes.len());

                info!("Starting benchmark phase 2 (certified blocks)");
                let certificates = context.make_benchmark_certificates_from_votes(votes);
                assert_eq!(
                    num_proposal,
                    certificates.len(),
                    "Unable to build all the expected certificates from received votes"
                );
                let messages = certificates
                    .iter()
                    .map(|certificate| {
                        RpcMessage::ConfirmedCertificate(Box::new(
                            HandleConfirmedCertificateRequest {
                                certificate: certificate.clone(),
                                wait_for_outgoing_messages: true,
                            },
                        ))
                    })
                    .collect();
                let responses = context
                    .mass_broadcast("certificates", max_in_flight, messages)
                    .await;
                let mut confirmed = HashSet::new();
                let num_valid = responses.into_iter().fold(0, |acc, message| {
                    match deserialize_response(message) {
                        Some(response) => {
                            confirmed.insert(response.info.chain_id);
                            acc + 1
                        }
                        None => acc,
                    }
                });
                info!(
                    "Confirmed {} valid certificates for {} block proposals.",
                    num_valid,
                    confirmed.len()
                );

                info!("Updating local state of user chains");
                context.update_wallet_from_certificates(certificates).await;
                context.save_wallet().await?;
            }

            Watch { chain_id, raw } => {
                let mut join_set = JoinSet::new();
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id)?;
                info!("Watching for notifications for chain {:?}", chain_id);
                let (listener, _listen_handle, mut notifications) = chain_client.listen().await?;
                join_set.spawn_task(listener);
                while let Some(notification) = notifications.next().await {
                    if let Reason::NewBlock { .. } = notification.reason {
                        context.update_and_save_wallet(&chain_client).await?;
                    }
                    if raw {
                        println!("{}", serde_json::to_string(&notification)?);
                    }
                }
                info!("Notification stream ended.");
            }

            Service { config, port } => {
                let default_chain = context.wallet().default_chain();
                let service = NodeService::new(config, port, default_chain, storage, context).await;
                service.run().await?;
            }

            Faucet {
                chain_id,
                port,
                amount,
                limit_rate_until,
                config,
            } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                info!("Starting faucet service using chain {}", chain_id);
                let end_timestamp = limit_rate_until
                    .map(|et| {
                        let micros = u64::try_from(et.timestamp_micros())
                            .expect("End timestamp before 1970");
                        Timestamp::from(micros)
                    })
                    .unwrap_or_else(Timestamp::now);
                let genesis_config = Arc::new(context.wallet().genesis_config().clone());
                let faucet = FaucetService::new(
                    port,
                    chain_id,
                    context,
                    amount,
                    end_timestamp,
                    genesis_config,
                    config,
                    storage,
                )
                .await?;
                faucet.run().await?;
            }

            PublishBytecode {
                contract,
                service,
                publisher,
            } => {
                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing bytecode on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher)?;
                let bytecode_id = context
                    .publish_bytecode(&chain_client, contract, service)
                    .await?;
                println!("{}", bytecode_id);
                info!(
                    "Bytecode published in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            PublishDataBlob {
                blob_path,
                publisher,
            } => {
                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing data blob on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher)?;
                let hash = context.publish_data_blob(&chain_client, blob_path).await?;
                println!("{}", hash);
                info!(
                    "Data blob published in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            // TODO(#2490): Consider removing or renaming this.
            ReadDataBlob { hash, reader } => {
                let start_time = Instant::now();
                let reader = reader.unwrap_or_else(|| context.default_chain());
                info!("Verifying data blob on chain {}", reader);
                let chain_client = context.make_chain_client(reader)?;
                context.read_data_blob(&chain_client, hash).await?;
                info!("Data blob read in {} ms", start_time.elapsed().as_millis());
            }

            CreateApplication {
                bytecode_id,
                creator,
                json_parameters,
                json_parameters_path,
                json_argument,
                json_argument_path,
                required_application_ids,
            } => {
                let start_time = Instant::now();
                let creator = creator.unwrap_or_else(|| context.default_chain());
                info!("Creating application on chain {}", creator);
                let chain_client = context.make_chain_client(creator)?;
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                info!("Synchronizing");
                let chain_client = chain_client;
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
                                    bytecode_id,
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
                publisher,
                json_parameters,
                json_parameters_path,
                json_argument,
                json_argument_path,
                required_application_ids,
            } => {
                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing and creating application on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher)?;
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                let bytecode_id = context
                    .publish_bytecode(&chain_client, contract, service)
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
                                    bytecode_id,
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

            RequestApplication {
                application_id,
                target_chain_id,
                requester_chain_id,
            } => {
                let start_time = Instant::now();
                let requester_chain_id =
                    requester_chain_id.unwrap_or_else(|| context.default_chain());
                info!("Requesting application for chain {}", requester_chain_id);
                let chain_client = context.make_chain_client(requester_chain_id)?;
                let certificate = context
                    .apply_client_command(&chain_client, |chain_client| {
                        let chain_client = chain_client.clone();
                        async move {
                            chain_client
                                .request_application(application_id, target_chain_id)
                                .await
                        }
                    })
                    .await
                    .context("Failed to request application")?;
                info!(
                    "Application requested in {} ms",
                    start_time.elapsed().as_millis()
                );
                debug!("{:?}", certificate);
            }

            Assign { owner, message_id } => {
                let start_time = Instant::now();
                let chain_id = ChainId::child(message_id);
                info!(
                    "Linking chain {chain_id} to its corresponding key in the wallet, owned by \
                    {owner}",
                );
                Self::assign_new_chain_to_key(
                    chain_id,
                    message_id,
                    storage,
                    owner,
                    None,
                    &mut context,
                )
                .await?;
                println!("{}", chain_id);
                context.save_wallet().await?;
                info!(
                    "Chain linked to owner in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            Project(project_command) => match project_command {
                ProjectCommand::PublishAndCreate {
                    path,
                    name,
                    publisher,
                    json_parameters,
                    json_parameters_path,
                    json_argument,
                    json_argument_path,
                    required_application_ids,
                } => {
                    let start_time = Instant::now();
                    let publisher = publisher.unwrap_or_else(|| context.default_chain());
                    info!("Creating application on chain {}", publisher);
                    let chain_client = context.make_chain_client(publisher)?;

                    let parameters = read_json(json_parameters, json_parameters_path)?;
                    let argument = read_json(json_argument, json_argument_path)?;
                    let project_path = path.unwrap_or_else(|| env::current_dir().unwrap());

                    let project = project::Project::from_existing_project(project_path)?;
                    let (contract_path, service_path) = project.build(name)?;

                    let bytecode_id = context
                        .publish_bytecode(&chain_client, contract_path, service_path)
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
                                        bytecode_id,
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
                let start_time = Instant::now();
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                info!("Committing pending block for chain {}", chain_id);
                let chain_client = context.make_chain_client(chain_id)?;
                match chain_client.process_pending_block().await? {
                    ClientOutcome::Committed(Some(certificate)) => {
                        info!("Pending block committed successfully.");
                        println!("{}", certificate.hash());
                    }
                    ClientOutcome::Committed(None) => info!("No block is currently pending."),
                    ClientOutcome::WaitForTimeout(timeout) => {
                        info!("Please try again at {}", timeout.timestamp)
                    }
                }
                context.update_and_save_wallet(&chain_client).await?;
                info!(
                    "Pending block retried in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            Wallet(WalletCommand::Init {
                faucet: Some(faucet_url),
                with_new_chain: true,
                with_other_chains,
                ..
            }) => {
                let start_time = Instant::now();
                let key_pair = context.wallet.generate_key_pair();
                let owner = key_pair.public().into();
                info!(
                    "Requesting a new chain for owner {owner} using the faucet at address \
                    {faucet_url}",
                );
                context
                    .wallet_mut()
                    .mutate(|w| w.add_unassigned_key_pair(key_pair))
                    .await?;
                let faucet = cli_wrappers::Faucet::new(faucet_url);
                let outcome = faucet.claim(&owner).await?;
                let validators = faucet.current_validators().await?;
                println!("{}", outcome.chain_id);
                println!("{}", outcome.message_id);
                println!("{}", outcome.certificate_hash);
                Self::assign_new_chain_to_key(
                    outcome.chain_id,
                    outcome.message_id,
                    storage.clone(),
                    owner,
                    Some(validators),
                    &mut context,
                )
                .await?;
                let admin_id = context.wallet().genesis_admin_chain();
                let chains = with_other_chains
                    .into_iter()
                    .chain([admin_id, outcome.chain_id]);
                Self::print_peg_certificate_hash(storage, chains, &context).await?;
                context
                    .wallet_mut()
                    .mutate(|w| w.set_default_chain(outcome.chain_id))
                    .await??;
                info!(
                    "Wallet initialized in {} ms",
                    start_time.elapsed().as_millis()
                );
            }

            CreateGenesisConfig { .. }
            | Keygen
            | Net(_)
            | Storage { .. }
            | Wallet(_)
            | ExtractScriptFromMarkdown { .. }
            | HelpMarkdown => {
                unreachable!()
            }
        }
        Ok(())
    }
}

impl Job {
    async fn assign_new_chain_to_key<S>(
        chain_id: ChainId,
        message_id: MessageId,
        storage: S,
        owner: Owner,
        validators: Option<Vec<(ValidatorName, String)>>,
        context: &mut ClientContext<S, impl Persist<Target = Wallet>>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let node_provider = context.make_node_provider();
        let client = client::Client::new(
            node_provider.clone(),
            storage,
            100,
            CrossChainMessageDelivery::Blocking,
            false,
            vec![message_id.chain_id, chain_id],
            "Temporary client for fetching the parent chain",
            NonZeroUsize::new(20).expect("Chain worker limit should not be zero"),
            DEFAULT_GRACE_PERIOD,
        );

        // Take the latest committee we know of.
        let admin_chain_id = context.wallet.genesis_admin_chain();
        let query = ChainInfoQuery::new(admin_chain_id).with_committees();
        let nodes: Vec<_> = if let Some(validators) = validators {
            node_provider
                .make_nodes_from_list(validators)?
                .map(|(name, node)| RemoteNode { name, node })
                .collect()
        } else {
            let info = client.local_node().handle_chain_info_query(query).await?;
            let committee = info
                .latest_committee()
                .context("Invalid chain info response; missing latest committee")?;
            node_provider
                .make_nodes(committee)?
                .map(|(name, node)| RemoteNode { name, node })
                .collect()
        };

        // Download the parent chain.
        let target_height = message_id.height.try_add_one()?;
        client
            .download_certificates(&nodes, message_id.chain_id, target_height)
            .await
            .context("Failed to download parent chain")?;

        // The initial timestamp for the new chain is taken from the block with the message.
        let certificate = client
            .local_node()
            .certificate_for(&message_id)
            .await
            .context("could not find OpenChain message")?;
        let executed_block = certificate.block();
        let Some(Message::System(SystemMessage::OpenChain(config))) = executed_block
            .message_by_id(&message_id)
            .map(|msg| &msg.message)
        else {
            bail!(
                "The message with the ID returned by the faucet is not OpenChain. \
                Please make sure you are connecting to a genuine faucet."
            );
        };
        anyhow::ensure!(
            config.ownership.verify_owner(&owner),
            "The chain with the ID returned by the faucet is not owned by you. \
            Please make sure you are connecting to a genuine faucet."
        );
        context
            .wallet_mut()
            .mutate(|w| {
                w.assign_new_chain_to_owner(owner, chain_id, executed_block.header.timestamp)
            })
            .await?
            .context("could not assign the new chain")?;
        Ok(())
    }

    /// Prints a warning message to explain that the wallet has been initialized using data from
    /// untrusted nodes, and gives instructions to verify that we are connected to the right
    /// network.
    async fn print_peg_certificate_hash<S>(
        storage: S,
        chain_ids: impl IntoIterator<Item = ChainId>,
        context: &ClientContext<S, impl Persist<Target = Wallet>>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let mut chains = HashMap::new();
        for chain_id in chain_ids {
            if chains.contains_key(&chain_id) {
                continue;
            }
            chains.insert(chain_id, storage.load_chain(chain_id).await?);
        }
        // Find a chain with the latest known epoch, preferably the admin chain.
        let (peg_chain_id, _) = chains
            .iter()
            .filter_map(|(chain_id, chain)| {
                let epoch = (*chain.execution_state.system.epoch.get())?;
                let is_admin = Some(*chain_id) == *chain.execution_state.system.admin_id.get();
                Some((*chain_id, (epoch, is_admin)))
            })
            .max_by_key(|(_, epoch)| *epoch)
            .context("no active chain found")?;
        let peg_chain = chains.remove(&peg_chain_id).unwrap();
        // These are the still-trusted committees. Every chain tip should be signed by one of them.
        let committees = peg_chain.execution_state.system.committees.get();
        for (chain_id, chain) in &chains {
            let Some(hash) = chain.tip_state.get().block_hash else {
                continue; // This chain was created based on the genesis config.
            };
            let certificate = storage.read_certificate(hash).await?;
            let committee = committees
                .get(&certificate.block().header.epoch)
                .ok_or_else(|| anyhow!("tip of chain {chain_id} is outdated."))?;
            certificate.check(committee)?;
        }
        // This proves that once we have verified that the peg chain's tip is a block in the real
        // network, we can be confident that all downloaded chains are.
        let config_hash = CryptoHash::new(context.wallet.genesis_config());
        let maybe_epoch = peg_chain.execution_state.system.epoch.get();
        let epoch = maybe_epoch.context("missing epoch in peg chain")?.0;
        info!(
            "Initialized wallet based on data provided by the faucet.\n\
            The current epoch is {epoch}.\n\
            The genesis config hash is {config_hash}{}",
            if let Some(peg_hash) = peg_chain.tip_state.get().block_hash {
                format!("\nThe latest certificate on chain {peg_chain_id} is {peg_hash}.")
            } else {
                "".to_string()
            }
        );
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let options = ClientOptions::init()?;

    linera_base::tracing::init(&log_file_name_for(&options.command));

    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    let span = tracing::info_span!("linera::main");
    if let Some(wallet_id) = options.with_wallet {
        span.record("wallet_id", wallet_id);
    }

    let result = runtime
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(run(&options).instrument(span));

    let error_code = match result {
        Ok(code) => code,
        Err(msg) => {
            tracing::error!("Error is {:?}", msg);
            2
        }
    };
    process::exit(error_code);
}

/// Returns the log file name to use based on the [`ClientCommand`] that will run.
fn log_file_name_for(command: &ClientCommand) -> Cow<'static, str> {
    match command {
        ClientCommand::Transfer { .. }
        | ClientCommand::OpenChain { .. }
        | ClientCommand::OpenMultiOwnerChain { .. }
        | ClientCommand::ChangeOwnership { .. }
        | ClientCommand::ChangeApplicationPermissions { .. }
        | ClientCommand::CloseChain { .. }
        | ClientCommand::LocalBalance { .. }
        | ClientCommand::QueryBalance { .. }
        | ClientCommand::SyncBalance { .. }
        | ClientCommand::Sync { .. }
        | ClientCommand::ProcessInbox { .. }
        | ClientCommand::QueryValidator { .. }
        | ClientCommand::QueryValidators { .. }
        | ClientCommand::SetValidator { .. }
        | ClientCommand::RemoveValidator { .. }
        | ClientCommand::ResourceControlPolicy { .. }
        | ClientCommand::FinalizeCommittee
        | ClientCommand::CreateGenesisConfig { .. }
        | ClientCommand::PublishBytecode { .. }
        | ClientCommand::PublishDataBlob { .. }
        | ClientCommand::ReadDataBlob { .. }
        | ClientCommand::CreateApplication { .. }
        | ClientCommand::PublishAndCreate { .. }
        | ClientCommand::RequestApplication { .. }
        | ClientCommand::Keygen { .. }
        | ClientCommand::Assign { .. }
        | ClientCommand::Wallet { .. }
        | ClientCommand::RetryPendingBlock { .. } => "client".into(),
        #[cfg(feature = "benchmark")]
        ClientCommand::Benchmark { .. } => "benchmark".into(),
        ClientCommand::Net { .. } => "net".into(),
        ClientCommand::Project { .. } => "project".into(),
        ClientCommand::Watch { .. } => "watch".into(),
        ClientCommand::Storage { .. } => "storage".into(),
        ClientCommand::Service { port, .. } => format!("service-{port}").into(),
        ClientCommand::Faucet { .. } => "faucet".into(),
        ClientCommand::HelpMarkdown | ClientCommand::ExtractScriptFromMarkdown { .. } => {
            "tool".into()
        }
    }
}

async fn run(options: &ClientOptions) -> Result<i32, anyhow::Error> {
    match &options.command {
        ClientCommand::HelpMarkdown => {
            clap_markdown::print_help_markdown::<ClientOptions>();
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

        ClientCommand::CreateGenesisConfig {
            committee_config_path,
            genesis_config_path,
            admin_root,
            initial_funding,
            start_timestamp,
            num_other_initial_chains,
            block_price,
            fuel_unit_price,
            read_operation_price,
            write_operation_price,
            byte_read_price,
            byte_written_price,
            byte_stored_price,
            operation_price,
            operation_byte_price,
            message_price,
            message_byte_price,
            maximum_fuel_per_block,
            maximum_executed_block_size,
            maximum_blob_size,
            maximum_bytecode_size,
            maximum_block_proposal_size,
            maximum_bytes_read_per_block,
            maximum_bytes_written_per_block,
            testing_prng_seed,
            network_name,
        } => {
            let start_time = Instant::now();
            let committee_config: CommitteeConfig = util::read_json(committee_config_path)
                .expect("Unable to read committee config file");
            let maximum_fuel_per_block = maximum_fuel_per_block.unwrap_or(u64::MAX);
            let maximum_bytes_read_per_block = maximum_bytes_read_per_block.unwrap_or(u64::MAX);
            let maximum_bytes_written_per_block =
                maximum_bytes_written_per_block.unwrap_or(u64::MAX);
            let maximum_executed_block_size = maximum_executed_block_size.unwrap_or(u64::MAX);
            let maximum_blob_size = maximum_blob_size.unwrap_or(u64::MAX);
            let maximum_bytecode_size = maximum_bytecode_size.unwrap_or(u64::MAX);
            let maximum_block_proposal_size = maximum_block_proposal_size.unwrap_or(u64::MAX);
            let policy = ResourceControlPolicy {
                block: *block_price,
                fuel_unit: *fuel_unit_price,
                read_operation: *read_operation_price,
                write_operation: *write_operation_price,
                byte_read: *byte_read_price,
                byte_written: *byte_written_price,
                byte_stored: *byte_stored_price,
                operation_byte: *operation_byte_price,
                operation: *operation_price,
                message_byte: *message_byte_price,
                message: *message_price,
                maximum_fuel_per_block,
                maximum_executed_block_size,
                maximum_blob_size,
                maximum_bytecode_size,
                maximum_block_proposal_size,
                maximum_bytes_read_per_block,
                maximum_bytes_written_per_block,
            };
            let timestamp = start_timestamp
                .map(|st| {
                    let micros =
                        u64::try_from(st.timestamp_micros()).expect("Start timestamp before 1970");
                    Timestamp::from(micros)
                })
                .unwrap_or_else(Timestamp::now);
            let admin_id = ChainId::root(*admin_root);
            let network_name = network_name.clone().unwrap_or_else(|| {
                // Default: e.g. "linera-2023-11-14T23:13:20"
                format!("linera-{}", Utc::now().naive_utc().format("%FT%T"))
            });
            let mut genesis_config = persistent::File::new(
                genesis_config_path,
                GenesisConfig::new(committee_config, admin_id, timestamp, policy, network_name),
            )?;
            let mut rng = Box::<dyn CryptoRng>::from(*testing_prng_seed);
            let mut chains = vec![];
            for i in 0..=*num_other_initial_chains {
                let description = ChainDescription::Root(i);
                // Create keys.
                let chain = UserChain::make_initial(&mut rng, description, timestamp);
                // Public "genesis" state.
                let key = chain.key_pair.as_ref().unwrap().public();
                genesis_config.chains.push((key, *initial_funding));
                // Private keys.
                chains.push(chain);
            }
            genesis_config.persist().await?;
            options
                .create_wallet(genesis_config.into_value(), *testing_prng_seed)?
                .mutate(|wallet| wallet.extend(chains))
                .await?;
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
                project.test().await?;
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
            let mut wallet = options.wallet().await?;
            let key_pair = wallet.generate_key_pair();
            let owner = Owner::from(key_pair.public());
            wallet
                .mutate(|w| w.add_unassigned_key_pair(key_pair))
                .await?;
            println!("{}", owner);
            info!("Key generated in {} ms", start_time.elapsed().as_millis());
            Ok(0)
        }

        ClientCommand::Net(net_command) => match net_command {
            #[cfg(feature = "kubernetes")]
            NetCommand::Up {
                extra_wallets,
                other_initial_chains,
                initial_amount,
                validators,
                shards,
                testing_prng_seed,
                policy_config,
                kubernetes: true,
                binaries,
                no_build,
                docker_image_name,
                path: _,
                storage: _,
                external_protocol: _,
                with_faucet_chain,
                faucet_port,
                faucet_amount,
            } => {
                net_up_utils::handle_net_up_kubernetes(
                    *extra_wallets,
                    *other_initial_chains,
                    *initial_amount,
                    *validators,
                    *shards,
                    *testing_prng_seed,
                    binaries,
                    *no_build,
                    docker_image_name.clone(),
                    policy_config.into_policy(),
                    *with_faucet_chain,
                    *faucet_port,
                    *faucet_amount,
                )
                .boxed()
                .await?;
                Ok(0)
            }

            NetCommand::Up {
                extra_wallets,
                other_initial_chains,
                initial_amount,
                validators,
                shards,
                testing_prng_seed,
                policy_config,
                path,
                storage,
                external_protocol,
                with_faucet_chain,
                faucet_port,
                faucet_amount,
                ..
            } => {
                net_up_utils::handle_net_up_service(
                    *extra_wallets,
                    *other_initial_chains,
                    *initial_amount,
                    *validators,
                    *shards,
                    *testing_prng_seed,
                    policy_config.into_policy(),
                    path,
                    storage,
                    external_protocol.clone(),
                    *with_faucet_chain,
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
                    "This will install a function `linera_spawn_and_read_wallet_variables` to facilitate \
                       testing with a local Linera network"
                );
                println!("{}", include_str!("../../template/linera_net_helper.sh"));
                Ok(0)
            }
        },

        ClientCommand::Storage(command) => {
            let storage_config = command.storage_config()?;
            let common_config = CommonStoreConfig::default();
            let full_storage_config = storage_config.add_common_config(common_config).await?;
            let start_time = Instant::now();
            match command {
                DatabaseToolCommand::DeleteAll { .. } => {
                    full_storage_config.delete_all().await?;
                    info!(
                        "All namespaces deleted in {} ms",
                        start_time.elapsed().as_millis()
                    );
                }
                DatabaseToolCommand::DeleteNamespace { .. } => {
                    full_storage_config.delete_namespace().await?;
                    info!(
                        "Namespace deleted in {} ms",
                        start_time.elapsed().as_millis()
                    );
                }
                DatabaseToolCommand::CheckExistence { .. } => {
                    let test = full_storage_config.test_existence().await?;
                    info!(
                        "Existence of a namespace checked in {} ms",
                        start_time.elapsed().as_millis()
                    );
                    if test {
                        println!("The database does exist");
                        return Ok(0);
                    } else {
                        println!("The database does not exist");
                        return Ok(1);
                    }
                }
                DatabaseToolCommand::CheckAbsence { .. } => {
                    let test = full_storage_config.test_existence().await?;
                    info!(
                        "Absence of a namespace checked in {} ms",
                        start_time.elapsed().as_millis()
                    );
                    if test {
                        println!("The database does exist");
                        return Ok(1);
                    } else {
                        println!("The database does not exist");
                        return Ok(0);
                    }
                }
                DatabaseToolCommand::Initialize { .. } => {
                    full_storage_config.initialize().await?;
                    info!(
                        "Initialization done in {} ms",
                        start_time.elapsed().as_millis()
                    );
                }
                DatabaseToolCommand::ListNamespaces { .. } => {
                    let namespaces = full_storage_config.list_all().await?;
                    info!(
                        "Namespaces listed in {} ms",
                        start_time.elapsed().as_millis()
                    );
                    println!("The list of namespaces is {:?}", namespaces);
                }
            }
            Ok(0)
        }

        ClientCommand::Wallet(wallet_command) => match wallet_command {
            WalletCommand::Show {
                chain_id,
                short,
                owned,
            } => {
                let start_time = Instant::now();
                let chain_ids = if let Some(chain_id) = chain_id {
                    ensure!(!owned, "Cannot specify both --owned and a chain ID");
                    vec![*chain_id]
                } else if *owned {
                    options.wallet().await?.owned_chain_ids()
                } else {
                    options.wallet().await?.chain_ids()
                };
                if *short {
                    for chain_id in chain_ids {
                        println!("{chain_id}");
                    }
                } else {
                    wallet::pretty_print(&*options.wallet().await?, chain_ids);
                }
                info!("Wallet shown in {} ms", start_time.elapsed().as_millis());
                Ok(0)
            }

            WalletCommand::SetDefault { chain_id } => {
                let start_time = Instant::now();
                options
                    .wallet()
                    .await?
                    .mutate(|w| w.set_default_chain(*chain_id))
                    .await??;
                info!(
                    "Default chain set in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }

            WalletCommand::ForgetKeys { chain_id } => {
                let start_time = Instant::now();
                options
                    .wallet()
                    .await?
                    .mutate(|w| w.forget_keys(chain_id))
                    .await??;
                info!(
                    "Chain keys forgotten in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }

            WalletCommand::ForgetChain { chain_id } => {
                let start_time = Instant::now();
                options
                    .wallet()
                    .await?
                    .mutate(|w| w.forget_chain(chain_id))
                    .await??;
                info!("Chain forgotten in {} ms", start_time.elapsed().as_millis());
                Ok(0)
            }

            WalletCommand::Init {
                genesis_config_path,
                faucet,
                with_new_chain,
                with_other_chains,
                testing_prng_seed,
            } => {
                let start_time = Instant::now();
                let genesis_config: GenesisConfig = match (genesis_config_path, faucet) {
                    (Some(genesis_config_path), None) => util::read_json(genesis_config_path)?,
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
                    (_, _) => bail!("Either --faucet or --genesis must be specified, but not both"),
                };
                let timestamp = genesis_config.timestamp;
                options
                    .create_wallet(genesis_config, *testing_prng_seed)?
                    .mutate(|wallet| {
                        wallet.extend(
                            with_other_chains
                                .iter()
                                .map(|chain_id| UserChain::make_other(*chain_id, timestamp)),
                        )
                    })
                    .await?;
                options.initialize_storage().boxed().await?;
                if *with_new_chain {
                    ensure!(
                        faucet.is_some(),
                        "Using --with-new-chain requires --faucet to be set"
                    );
                    options.run_with_storage(Job(options.clone())).await??;
                }
                info!(
                    "Wallet initialized in {} ms",
                    start_time.elapsed().as_millis()
                );
                Ok(0)
            }
        },

        _ => {
            options.run_with_storage(Job(options.clone())).await??;
            Ok(0)
        }
    }
}
