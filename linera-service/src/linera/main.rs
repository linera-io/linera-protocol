// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(clippy::large_futures)]

use std::{collections::HashMap, env, path::PathBuf, sync::Arc, time::Instant};

use anyhow::{anyhow, bail, ensure, Context};
use async_trait::async_trait;
use chrono::Utc;
use client_context::ClientContext;
use client_options::ClientOptions;
use colored::Colorize;
use futures::{lock::Mutex, FutureExt as _, StreamExt};
use linera_base::{
    crypto::{CryptoHash, CryptoRng, PublicKey},
    data_types::{ApplicationPermissions, Timestamp},
    identifiers::{ChainDescription, ChainId, MessageId, Owner},
    ownership::ChainOwnership,
};
use linera_chain::data_types::{CertificateValue, ExecutedBlock};
use linera_core::{
    client::ChainClientError,
    data_types::{ChainInfoQuery, ClientOutcome},
    local_node::LocalNodeClient,
    node::LocalValidatorNodeProvider,
    worker::{Reason, WorkerState},
    JoinSetExt as _,
};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    system::{SystemChannel, UserData},
    Message, ResourceControlPolicy, SystemMessage,
};
use linera_service::{
    chain_listener::ClientContext as _,
    cli_wrappers,
    config::{CommitteeConfig, Export, GenesisConfig, Import, WalletState},
    faucet::FaucetService,
    node_service::NodeService,
    project::{self, Project},
    storage::Runnable,
    wallet::UserChain,
};
use linera_storage::Storage;
use linera_views::views::ViewError;
use serde_json::Value;
use tokio::task::JoinSet;
use tracing::{debug, info, warn};

mod client_context;
mod client_options;
mod net_up_utils;

#[cfg(feature = "benchmark")]
use {
    linera_chain::data_types::HashedCertificateValue,
    linera_core::data_types::ChainInfoResponse,
    linera_rpc::{HandleCertificateRequest, RpcMessage},
    std::collections::HashSet,
    tracing::error,
};

use crate::client_options::{ClientCommand, NetCommand, ProjectCommand, WalletCommand};

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

struct Job(ClientOptions, WalletState);

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
    type Output = ();

    async fn run<S>(self, storage: S) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let Job(options, wallet) = self;
        let mut context = ClientContext::new(storage.clone(), &options, wallet);
        let command = options.command;

        use ClientCommand::*;
        match command {
            Transfer {
                sender,
                recipient,
                amount,
            } => {
                let chain_client = context.make_chain_client(sender.chain_id).into_arc();
                info!(
                    "Starting transfer of {} native tokens from {} to {}",
                    amount, sender, recipient
                );
                let time_start = Instant::now();
                let certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| async move {
                        let data = UserData::default();
                        chain_client
                            .transfer_to_account(sender.owner, amount, recipient, data)
                            .await
                    })
                    .await
                    .context("Failed to make transfer")?;
                let time_total = time_start.elapsed();
                info!("Operation confirmed after {} ms", time_total.as_millis());
                debug!("{:?}", certificate);
            }

            OpenChain {
                chain_id,
                public_key,
                balance,
            } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).into_arc();
                let (new_public_key, key_pair) = match public_key {
                    Some(key) => (key, None),
                    None => {
                        let key_pair = context.wallet_state.generate_key_pair();
                        (key_pair.public(), Some(key_pair))
                    }
                };
                info!("Opening a new chain from existing chain {}", chain_id);
                let time_start = Instant::now();
                let (message_id, certificate) = context
                    .apply_client_command(&chain_client, |mut chain_client| {
                        let ownership = ChainOwnership::single(new_public_key);
                        async move {
                            chain_client
                                .open_chain(ownership, ApplicationPermissions::default(), balance)
                                .await
                        }
                    })
                    .await
                    .context("Failed to open chain")?;
                let id = ChainId::child(message_id);
                let timestamp = match certificate.value() {
                    CertificateValue::ConfirmedBlock {
                        executed_block: ExecutedBlock { block, .. },
                        ..
                    } => block.timestamp,
                    _ => panic!("Unexpected certificate."),
                };
                context.update_wallet_for_new_chain(id, key_pair, timestamp);
                context.save_wallet();
                let time_total = time_start.elapsed();
                info!("Operation confirmed after {} ms", time_total.as_millis());
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
                let chain_client = context.make_chain_client(chain_id).into_arc();
                info!(
                    "Opening a new multi-owner chain from existing chain {}",
                    chain_id
                );
                let time_start = Instant::now();
                let ownership = ChainOwnership::try_from(ownership_config)?;
                let application_permissions =
                    ApplicationPermissions::from(application_permissions_config);
                let (message_id, certificate) = context
                    .apply_client_command(&chain_client, |mut chain_client| {
                        let ownership = ownership.clone();
                        let application_permissions = application_permissions.clone();
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
                let timestamp = match certificate.value() {
                    CertificateValue::ConfirmedBlock {
                        executed_block: ExecutedBlock { block, .. },
                        ..
                    } => block.timestamp,
                    _ => panic!("Unexpected certificate."),
                };
                context.update_wallet_for_new_chain(id, key_pair, timestamp);
                context.save_wallet();
                let time_total = time_start.elapsed();
                info!("Operation confirmed after {} ms", time_total.as_millis());
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
                let chain_client = context.make_chain_client(chain_id).into_arc();
                info!("Changing application permissions for chain {}", chain_id);
                let time_start = Instant::now();
                let application_permissions =
                    ApplicationPermissions::from(application_permissions_config);
                let certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| {
                        let application_permissions = application_permissions.clone();
                        async move {
                            chain_client
                                .change_application_permissions(application_permissions)
                                .await
                        }
                    })
                    .await
                    .context("Failed to change application permissions")?;
                let time_total = time_start.elapsed();
                info!("Operation confirmed after {} ms", time_total.as_millis());
                debug!("{:?}", certificate);
            }

            CloseChain { chain_id } => {
                let chain_client = context.make_chain_client(chain_id).into_arc();
                info!("Closing chain {}", chain_id);
                let time_start = Instant::now();
                let certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| async move {
                        chain_client.close_chain().await
                    })
                    .await
                    .context("Failed to close chain")?;
                let time_total = time_start.elapsed();
                info!("Operation confirmed after {} ms", time_total.as_millis());
                debug!("{:?}", certificate);
            }

            Subscribe {
                subscriber,
                publisher,
                channel,
            } => {
                let subscriber = subscriber.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(subscriber).into_arc();
                let time_start = Instant::now();
                info!("Subscribing");
                let certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| async move {
                        match channel {
                            SystemChannel::Admin => {
                                chain_client.subscribe_to_new_committees().await
                            }
                            SystemChannel::PublishedBytecodes => {
                                let publisher = publisher.ok_or_else(|| {
                                    ChainClientError::InternalError("Incorrect chain ID")
                                })?;
                                chain_client
                                    .subscribe_to_published_bytecodes(publisher)
                                    .await
                            }
                        }
                    })
                    .await
                    .context("Failed to subscribe")?;
                let time_total = time_start.elapsed();
                info!("Subscription confirmed after {} ms", time_total.as_millis());
                debug!("{:?}", certificate);
            }

            Unsubscribe {
                subscriber,
                publisher,
                channel,
            } => {
                let subscriber = subscriber.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(subscriber).into_arc();
                let time_start = Instant::now();
                let certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| async move {
                        match channel {
                            SystemChannel::Admin => {
                                info!("Unsubscribing from admin channel");
                                chain_client.unsubscribe_from_new_committees().await
                            }
                            SystemChannel::PublishedBytecodes => {
                                let publisher = publisher.ok_or_else(|| {
                                    ChainClientError::InternalError("Incorrect chain ID")
                                })?;
                                info!("Unsubscribing from publisher {}", publisher);
                                chain_client
                                    .unsubscribe_from_published_bytecodes(publisher)
                                    .await
                            }
                        }
                    })
                    .await
                    .context("Failed to unsubscribe")?;
                let time_total = time_start.elapsed();
                info!("Unsubscribed in {} ms", time_total.as_millis());
                debug!("{:?}", certificate);
            }

            LocalBalance { account } => {
                let account = account.unwrap_or_else(|| context.default_account());
                let mut chain_client = context.make_chain_client(account.chain_id);
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
                let mut chain_client = context.make_chain_client(account.chain_id);
                info!(
                    "Evaluating the local balance of {} by staging execution of known incoming messages", account
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
                let mut chain_client = context.make_chain_client(account.chain_id);
                info!("Synchronizing chain information and querying the local balance");
                warn!("This command is deprecated. Use `linera sync && linera query-balance` instead.");
                let time_start = Instant::now();
                chain_client.synchronize_from_validators().await?;
                let result = match account.owner {
                    Some(owner) => chain_client.query_owner_balance(owner).await,
                    None => chain_client.query_balance().await,
                };
                context.update_and_save_wallet(&mut chain_client).await;
                let balance = result.context("Failed to synchronize from validators")?;
                let time_total = time_start.elapsed();
                info!("Operation confirmed after {} ms", time_total.as_millis());
                println!("{}", balance);
            }

            Sync { chain_id } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let mut chain_client = context.make_chain_client(chain_id);
                info!("Synchronizing chain information");
                let time_start = Instant::now();
                chain_client.synchronize_from_validators().await?;
                context.update_and_save_wallet(&mut chain_client).await;
                let time_total = time_start.elapsed();
                info!(
                    "Synchronized chain information in {} ms",
                    time_total.as_millis()
                );
            }

            ProcessInbox { chain_id } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).into_arc();
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

            QueryValidators { chain_id } => {
                use linera_core::node::ValidatorNode as _;

                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let mut chain_client = context.make_chain_client(chain_id);
                info!(
                    "Querying the validators of the current epoch of chain {}",
                    chain_id
                );
                let time_start = Instant::now();
                let result = chain_client.local_committee().await;
                context.update_and_save_wallet(&mut chain_client).await;
                let committee = result.context("Failed to get local committee")?;
                let time_total = time_start.elapsed();
                info!("Validators obtained after {} ms", time_total.as_millis());
                info!("{:?}", committee.validators());
                let node_provider = context.make_node_provider();
                for (name, state) in committee.validators() {
                    match node_provider
                        .make_node(&state.network_address)?
                        .get_version_info()
                        .await
                    {
                        Ok(version_info) => {
                            info!(
                                "Version information for validator {name:?}:{}",
                                version_info
                            );
                        }
                        Err(e) => {
                            warn!("Failed to get version information for validator {name:?}:\n{e}")
                        }
                    }
                }
            }

            command @ (SetValidator { .. }
            | RemoveValidator { .. }
            | ResourceControlPolicy { .. }) => {
                info!("Starting operations to change validator set");
                let time_start = Instant::now();

                // Make sure genesis chains are subscribed to the admin chain.
                let context = Arc::new(Mutex::new(context));
                let mut context = context.lock().await;
                let chain_client = context
                    .make_chain_client(context.wallet().genesis_admin_chain())
                    .into_arc();
                let n = context
                    .process_inbox(&chain_client)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter_map(|c| c.value().executed_block().map(|e| e.messages().len()))
                    .sum::<usize>();
                info!("Subscribed {} chains to new committees", n);
                let maybe_certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| {
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
                                    info!(
                                        "ResourceControlPolicy:\n\
                            {:.2} base cost per block\n\
                            {:.2} cost per fuel unit\n\
                            {:.2} cost per read operation\n\
                            {:.2} cost per write operation\n\
                            {:.2} cost per byte read\n\
                            {:.2} cost per byte written\n\
                            {:.2} cost per byte stored\n\
                            {:.2} per operation\n\
                            {:.2} per byte in the argument of an operation\n\
                            {:.2} per outgoing messages\n\
                            {:.2} per byte in the argument of an outgoing messages\n\
                            {:.2} maximum number bytes read per block\n\
                            {:.2} maximum number bytes written per block",
                                        policy.block,
                                        policy.fuel_unit,
                                        policy.read_operation,
                                        policy.write_operation,
                                        policy.byte_read,
                                        policy.byte_written,
                                        policy.byte_stored,
                                        policy.operation,
                                        policy.operation_byte,
                                        policy.message,
                                        policy.message_byte,
                                        policy.maximum_bytes_read_per_block,
                                        policy.maximum_bytes_written_per_block
                                    );
                                    if block.is_none()
                                        && fuel_unit.is_none()
                                        && read_operation.is_none()
                                        && write_operation.is_none()
                                        && byte_read.is_none()
                                        && byte_written.is_none()
                                        && byte_stored.is_none()
                                        && operation.is_none()
                                        && operation_byte.is_none()
                                        && message.is_none()
                                        && message_byte.is_none()
                                        && maximum_bytes_read_per_block.is_none()
                                        && maximum_bytes_written_per_block.is_none()
                                    {
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

                // Remove the old committee.
                info!("Finalizing committee:\n{:?}", certificate);
                context
                    .apply_client_command(&chain_client, |mut chain_client| async move {
                        chain_client.finalize_committee().await
                    })
                    .await
                    .context("Failed to finalize committee")?;
                context.save_wallet();

                let time_total = time_start.elapsed();
                info!("Operations confirmed after {} ms", time_total.as_millis());
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
                        let (executed_block, _) =
                            WorkerState::new("staging".to_string(), None, storage.clone())
                                .stage_block_execution(proposal.content.block.clone())
                                .await?;
                        let value =
                            HashedCertificateValue::from(CertificateValue::ConfirmedBlock {
                                executed_block,
                            });
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
                        HandleCertificateRequest {
                            certificate: certificate.clone(),
                            hashed_certificate_values: vec![],
                            hashed_blobs: vec![],
                            wait_for_outgoing_messages: true,
                        }
                        .into()
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
                context.save_wallet();
            }

            Watch { chain_id, raw } => {
                let mut join_set = JoinSet::new();
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                let chain_client = context.make_chain_client(chain_id).into_arc();
                info!("Watching for notifications for chain {:?}", chain_id);
                let (listener, _listen_handle, mut notifications) = chain_client.listen().await?;
                join_set.spawn_task(listener);
                while let Some(notification) = notifications.next().await {
                    if let Reason::NewBlock { .. } = notification.reason {
                        let mut guard = chain_client.lock().await;
                        context.update_and_save_wallet(&mut *guard).await;
                    }
                    if raw {
                        println!("{}", serde_json::to_string(&notification)?);
                    }
                }
                info!("Notification stream ended.");
            }

            Service { config, port } => {
                let default_chain = context.wallet().default_chain();
                let service = NodeService::new(config, port, default_chain, storage, context);
                service.run().await?;
            }

            Faucet {
                chain_id,
                port,
                amount,
                limit_rate_until,
            } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                info!("Starting faucet service using chain {}", chain_id);
                let chain_client = context.make_chain_client(chain_id);
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
                    chain_client,
                    context,
                    amount,
                    end_timestamp,
                    genesis_config,
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
                let chain_client = context.make_chain_client(publisher).into_arc();
                let bytecode_id = context
                    .publish_bytecode(&chain_client, contract, service)
                    .await?;
                println!("{}", bytecode_id);
                info!("{}", "Bytecode published successfully!".green().bold());
                info!("Time elapsed: {} ms", start_time.elapsed().as_millis());
            }

            PublishBlob {
                blob_path,
                publisher,
            } => {
                let start_time = Instant::now();
                let publisher = publisher.unwrap_or_else(|| context.default_chain());
                info!("Publishing blob on chain {}", publisher);
                let chain_client = context.make_chain_client(publisher).into_arc();
                let blob_id = context.publish_blob(&chain_client, blob_path).await?;
                println!("{}", blob_id);
                info!("{}", "Blob published successfully!".green().bold());
                info!("Time elapsed: {} ms", start_time.elapsed().as_millis());
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
                let mut chain_client = context.make_chain_client(creator);
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                info!("Synchronizing");
                chain_client.synchronize_from_validators().await?;
                let chain_client = chain_client.into_arc();
                context.process_inbox(&chain_client).await?;

                let (application_id, _) = context
                    .apply_client_command(&chain_client, move |mut chain_client| {
                        let parameters = parameters.clone();
                        let argument = argument.clone();
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
                info!("Time elapsed: {} ms", start_time.elapsed().as_millis());
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
                let chain_client = context.make_chain_client(publisher).into_arc();
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                let bytecode_id = context
                    .publish_bytecode(&chain_client, contract, service)
                    .await?;

                let (application_id, _) = context
                    .apply_client_command(&chain_client, move |mut chain_client| {
                        let parameters = parameters.clone();
                        let argument = argument.clone();
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
                info!("Time elapsed: {} ms", start_time.elapsed().as_millis());
                println!("{}", application_id);
            }

            RequestApplication {
                application_id,
                target_chain_id,
                requester_chain_id,
            } => {
                let requester_chain_id =
                    requester_chain_id.unwrap_or_else(|| context.default_chain());
                info!("Requesting application for chain {}", requester_chain_id);
                let chain_client = context.make_chain_client(requester_chain_id).into_arc();
                let certificate = context
                    .apply_client_command(&chain_client, |mut chain_client| async move {
                        chain_client
                            .request_application(application_id, target_chain_id)
                            .await
                    })
                    .await
                    .context("Failed to request application")?;
                debug!("{:?}", certificate);
            }

            Assign { key, message_id } => {
                let chain_id = ChainId::child(message_id);
                info!(
                    "Linking chain {} to its corresponding key in the wallet, owned by {}",
                    chain_id,
                    Owner::from(&key)
                );
                Self::assign_new_chain_to_key(
                    chain_id,
                    message_id,
                    storage,
                    key,
                    None,
                    &mut context,
                )
                .await?;
                println!("{}", chain_id);
                context.save_wallet();
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
                    let chain_client = context.make_chain_client(publisher).into_arc();

                    let parameters = read_json(json_parameters, json_parameters_path)?;
                    let argument = read_json(json_argument, json_argument_path)?;
                    let project_path = path.unwrap_or_else(|| env::current_dir().unwrap());

                    let project = project::Project::from_existing_project(project_path)?;
                    let (contract_path, service_path) = project.build(name)?;

                    let bytecode_id = context
                        .publish_bytecode(&chain_client, contract_path, service_path)
                        .await?;

                    let (application_id, _) = context
                        .apply_client_command(&chain_client, move |mut chain_client| {
                            let parameters = parameters.clone();
                            let argument = argument.clone();
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
                    info!("Time elapsed: {} ms", start_time.elapsed().as_millis());
                    println!("{}", application_id);
                }
                _ => unreachable!("other project commands do not require storage"),
            },

            RetryPendingBlock { chain_id } => {
                let chain_id = chain_id.unwrap_or_else(|| context.default_chain());
                info!("Committing pending block for chain {}", chain_id);
                let mut chain_client = context.make_chain_client(chain_id);
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
                context.update_and_save_wallet(&mut chain_client).await;
            }

            Wallet(WalletCommand::Init {
                faucet: Some(faucet_url),
                with_new_chain: true,
                with_other_chains,
                ..
            }) => {
                let key_pair = context.wallet_state.generate_key_pair();
                let public_key = key_pair.public();
                info!(
                    "Requesting a new chain for owner {} using the faucet at address {}",
                    Owner::from(&public_key),
                    faucet_url,
                );
                context.wallet_mut().add_unassigned_key_pair(key_pair);
                let faucet = cli_wrappers::Faucet::new(faucet_url);
                let outcome = faucet.claim(&public_key).await?;
                let validators = faucet.current_validators().await?;
                println!("{}", outcome.chain_id);
                println!("{}", outcome.message_id);
                println!("{}", outcome.certificate_hash);
                Self::assign_new_chain_to_key(
                    outcome.chain_id,
                    outcome.message_id,
                    storage.clone(),
                    public_key,
                    Some(validators),
                    &mut context,
                )
                .await?;
                let admin_id = context.wallet().genesis_admin_chain();
                let chains = with_other_chains
                    .into_iter()
                    .chain([admin_id, outcome.chain_id]);
                Self::print_peg_certificate_hash(storage, chains, &context).await?;
                context.wallet_mut().set_default_chain(outcome.chain_id)?;
                context.save_wallet();
            }

            CreateGenesisConfig { .. } | Keygen | Net(_) | Wallet(_) | HelpMarkdown => {
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
        public_key: PublicKey,
        validators: Option<Vec<(ValidatorName, String)>>,
        context: &mut ClientContext<S>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let state = WorkerState::new("Local node".to_string(), None, storage)
            .with_allow_inactive_chains(true)
            .with_allow_messages_from_deprecated_epochs(true);
        let node_client = LocalNodeClient::new(state);

        // Take the latest committee we know of.
        let admin_chain_id = context.wallet().genesis_admin_chain();
        let query = ChainInfoQuery::new(admin_chain_id).with_committees();
        let nodes = if let Some(validators) = validators {
            context
                .make_node_provider()
                .make_nodes_from_list(validators)?
        } else {
            let info = node_client.handle_chain_info_query(query).await?;
            let committee = info
                .latest_committee()
                .context("Invalid chain info response; missing latest committee")?;
            context.make_node_provider().make_nodes(committee)?
        };

        // Download the parent chain.
        let target_height = message_id.height.try_add_one()?;
        node_client
            .download_certificates(nodes, message_id.chain_id, target_height, &mut vec![])
            .await
            .context("Failed to download parent chain")?;

        // The initial timestamp for the new chain is taken from the block with the message.
        let certificate = node_client
            .certificate_for(&message_id)
            .await
            .context("could not find OpenChain message")?;
        let CertificateValue::ConfirmedBlock { executed_block, .. } = certificate.value() else {
            bail!(
                "Unexpected certificate. Please make sure you are connecting to the right \
                network and are using a current software version."
            );
        };
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
            config.ownership.verify_owner(&Owner::from(public_key)) == Some(public_key),
            "The chain with the ID returned by the faucet is not owned by you. \
            Please make sure you are connecting to a genuine faucet."
        );
        context
            .wallet_mut()
            .assign_new_chain_to_key(public_key, chain_id, executed_block.block.timestamp)
            .context("could not assign the new chain")?;
        Ok(())
    }

    /// Prints a warning message to explain that the wallet has been initialized using data from
    /// untrusted nodes, and gives instructions to verify that we are connected to the right
    /// network.
    async fn print_peg_certificate_hash<S>(
        storage: S,
        chain_ids: impl IntoIterator<Item = ChainId>,
        context: &ClientContext<S>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
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
                .get(&certificate.value().epoch())
                .ok_or_else(|| anyhow!("tip of chain {chain_id} is outdated."))?;
            certificate.check(committee)?;
        }
        // This proves that once we have verified that the peg chain's tip is a block in the real
        // network, we can be confident that all downloaded chains are.
        let config_hash = CryptoHash::new(context.wallet().genesis_config());
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
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();
    let options = ClientOptions::init()?;

    let mut runtime = if options.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();

        if let Some(threads) = options.tokio_threads {
            builder.worker_threads(threads);
        }

        builder
    };

    runtime
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
        .block_on(run(options))
}

async fn run(options: ClientOptions) -> anyhow::Result<()> {
    match &options.command {
        ClientCommand::HelpMarkdown => {
            clap_markdown::print_help_markdown::<ClientOptions>();
            Ok(())
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
            maximum_bytes_read_per_block,
            maximum_bytes_written_per_block,
            testing_prng_seed,
            network_name,
        } => {
            let committee_config = CommitteeConfig::read(committee_config_path)
                .expect("Unable to read committee config file");
            let maximum_bytes_read_per_block = match *maximum_bytes_read_per_block {
                Some(value) => value,
                None => u64::MAX,
            };
            let maximum_bytes_written_per_block = match *maximum_bytes_written_per_block {
                Some(value) => value,
                None => u64::MAX,
            };
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
                // Default: e.g. "linera-test-2023-11-14T23:13:20"
                format!("linera-test-{}", Utc::now().naive_utc().format("%FT%T"))
            });
            let mut genesis_config =
                GenesisConfig::new(committee_config, admin_id, timestamp, policy, network_name);
            let mut rng = Box::<dyn CryptoRng>::from(*testing_prng_seed);
            let mut chains = vec![];
            for i in 0..*num_other_initial_chains {
                let description = ChainDescription::Root(i);
                // Create keys.
                let chain = UserChain::make_initial(&mut rng, description, timestamp);
                // Public "genesis" state.
                let key = chain.key_pair.as_ref().unwrap().public();
                genesis_config.chains.push((key, *initial_funding));
                // Private keys.
                chains.push(chain);
            }
            genesis_config.write(genesis_config_path)?;
            let mut wallet_state = options.create_wallet(genesis_config, *testing_prng_seed)?;
            wallet_state.extend(chains);
            wallet_state.save()?;
            options.initialize_storage().boxed().await?;
            Ok(())
        }

        ClientCommand::Project(project_command) => match project_command {
            ProjectCommand::New { name, linera_root } => {
                Project::create_new(name, linera_root.as_ref().map(AsRef::as_ref))?;
                Ok(())
            }
            ProjectCommand::Test { path } => {
                let path = path.clone().unwrap_or_else(|| env::current_dir().unwrap());
                let project = Project::from_existing_project(path)?;
                Ok(project.test().await?)
            }
            ProjectCommand::PublishAndCreate { .. } => options.run_command_with_storage().await,
        },

        ClientCommand::Keygen => {
            let mut wallet = options.wallet()?;
            let key_pair = wallet.generate_key_pair();
            let public = key_pair.public();
            wallet.inner_mut().add_unassigned_key_pair(key_pair);
            wallet.save()?;
            println!("{}", public);
            Ok(())
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
                table_name: _,
                kubernetes: true,
                binaries,
            } => {
                net_up_utils::handle_net_up_kubernetes(
                    *extra_wallets,
                    *other_initial_chains,
                    *initial_amount,
                    *validators,
                    *shards,
                    *testing_prng_seed,
                    binaries,
                )
                .await
            }

            NetCommand::Up {
                extra_wallets,
                other_initial_chains,
                initial_amount,
                validators,
                shards,
                testing_prng_seed,
                table_name,
                ..
            } => {
                net_up_utils::handle_net_up_service(
                    *extra_wallets,
                    *other_initial_chains,
                    *initial_amount,
                    *validators,
                    *shards,
                    *testing_prng_seed,
                    table_name,
                )
                .await
            }

            NetCommand::Helper => {
                info!("You may append the following script to your `~/.bash_profile` or `source` it when needed.");
                info!(
                    "This will install a function `linera_spawn_and_read_wallet_variables` to facilitate \
                       testing with a local Linera network"
                );
                println!("{}", include_str!("../../template/linera_net_helper.sh"));
                Ok(())
            }
        },

        ClientCommand::Wallet(wallet_command) => match wallet_command {
            WalletCommand::Show { chain_id } => {
                options.wallet()?.inner().pretty_print(*chain_id);
                Ok(())
            }

            WalletCommand::SetDefault { chain_id } => {
                let mut wallet = options.wallet()?;
                wallet.inner_mut().set_default_chain(*chain_id)?;
                wallet.save()?;
                Ok(())
            }

            WalletCommand::ForgetKeys { chain_id } => {
                let mut wallet = options.wallet()?;
                wallet.inner_mut().forget_keys(chain_id)?;
                wallet.save()?;
                Ok(())
            }

            WalletCommand::ForgetChain { chain_id } => {
                let mut wallet = options.wallet()?;
                wallet.inner_mut().forget_chain(chain_id)?;
                wallet.save()?;
                Ok(())
            }

            WalletCommand::Init {
                genesis_config_path,
                faucet,
                with_new_chain,
                with_other_chains,
                testing_prng_seed,
            } => {
                let genesis_config = match (genesis_config_path, faucet) {
                    (Some(genesis_config_path), None) => GenesisConfig::read(genesis_config_path)?,
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
                let mut wallet = options.create_wallet(genesis_config, *testing_prng_seed)?;
                wallet.extend(
                    with_other_chains
                        .iter()
                        .map(|chain_id| UserChain::make_other(*chain_id, timestamp)),
                );
                wallet.save()?;
                options.initialize_storage().boxed().await?;
                if *with_new_chain {
                    ensure!(
                        faucet.is_some(),
                        "Using --with-new-chain requires --faucet to be set"
                    );
                    options.run_command_with_storage().await?;
                }
                Ok(())
            }
        },

        _ => options.run_command_with_storage().await,
    }
}
