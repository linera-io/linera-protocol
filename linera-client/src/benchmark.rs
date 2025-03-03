// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashMap, iter};

use linera_base::{
    crypto::{AccountPublicKey, AccountSecretKey},
    data_types::{Amount, Timestamp},
    hashed::Hashed,
    identifiers::{AccountOwner, ApplicationId, ChainId, Owner},
    listen_for_shutdown_signals,
    time::Instant,
};
use linera_chain::{
    data_types::{BlockProposal, ProposedBlock},
    types::ConfirmedBlock,
};
use linera_core::{client::ChainClient, local_node::LocalNodeClient};
use linera_execution::{
    committee::{Committee, Epoch},
    system::{Recipient, SystemOperation},
    Operation,
};
use linera_rpc::node_provider::NodeProvider;
use linera_sdk::abis::fungible;
use linera_storage::Storage;
use num_format::{Locale, ToFormattedString};
use tokio::{runtime::Handle, task, time};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn, Instrument as _};

use crate::Error;

pub struct Benchmark<Storage>
where
    Storage: linera_storage::Storage,
{
    _phantom: std::marker::PhantomData<Storage>,
}

impl<S> Benchmark<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[allow(clippy::too_many_arguments)]
    pub async fn run_benchmark(
        num_chains: usize,
        transactions_per_block: usize,
        bps: Option<usize>,
        chain_clients: HashMap<ChainId, ChainClient<NodeProvider, S>>,
        epoch: Epoch,
        blocks_infos: Vec<(ChainId, Vec<Operation>, AccountSecretKey)>,
        committee: Committee,
        local_node: LocalNodeClient<S>,
        close_chains: bool,
    ) -> Result<(), Error> {
        let shutdown_notifier = CancellationToken::new();
        tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));
        let handle = Handle::current();

        // The bps control task will control the BPS from the threads. `crossbeam_channel` is used
        // for two reasons:
        // 1. it allows bounded channels with zero sized buffers.
        // 2. it blocks the current thread if the message can't be sent, which is exactly
        //    what we want to happen here. `tokio::sync::mpsc` doesn't do that. `std::sync::mpsc`
        //    does, but it is slower than `crossbeam_channel`.
        // Number 1 is the main reason. `tokio::sync::mpsc` doesn't allow 0 sized buffers.
        // With a channel with a buffer of size 1 or larger, even if we have already reached
        // the desired BPS, the tasks would continue sending block proposals until the channel's
        // buffer is filled, which would cause us to not properly control the BPS rate.
        let (sender, receiver) = crossbeam_channel::bounded(0);
        let bps_control_task = tokio::task::spawn_blocking(move || {
            handle.block_on(async move {
                let mut recv_count = 0;
                let mut start = time::Instant::now();
                while let Ok(()) = receiver.recv() {
                    recv_count += 1;
                    if recv_count == num_chains {
                        let elapsed = start.elapsed();
                        if let Some(bps) = bps {
                            let tps =
                                (bps * transactions_per_block).to_formatted_string(&Locale::en);
                            let bps = bps.to_formatted_string(&Locale::en);
                            if elapsed > time::Duration::from_secs(1) {
                                warn!(
                                    "Failed to achieve {} BPS/{} TPS in {} ms",
                                    bps,
                                    tps,
                                    elapsed.as_millis(),
                                );
                            } else {
                                time::sleep(time::Duration::from_secs(1) - elapsed).await;
                                info!(
                                    "Achieved {} BPS/{} TPS in {} ms",
                                    bps,
                                    tps,
                                    elapsed.as_millis(),
                                );
                            }
                        } else {
                            let achieved_bps = num_chains as f64 / elapsed.as_secs_f64();
                            info!(
                                "Achieved {} BPS/{} TPS in {} ms",
                                achieved_bps,
                                achieved_bps * transactions_per_block as f64,
                                elapsed.as_millis(),
                            );
                        }

                        recv_count = 0;
                        start = time::Instant::now();
                    }
                }

                info!("Exiting logging task...");
            })
        });

        let mut bps_remainder = bps.unwrap_or_default() % num_chains;
        let bps_share = bps.map(|bps| bps / num_chains);

        let mut join_set = task::JoinSet::<Result<(), Error>>::new();
        for (chain_id, operations, key_pair) in blocks_infos {
            let bps_share = if bps_remainder > 0 {
                bps_remainder -= 1;
                Some(bps_share.unwrap() + 1)
            } else {
                bps_share
            };

            let shutdown_notifier = shutdown_notifier.clone();
            let sender = sender.clone();
            let handle = Handle::current();
            let committee = committee.clone();
            let local_node = local_node.clone();
            let chain_client = chain_clients[&chain_id].clone();
            let short_chain_id = format!("{:?}", chain_id);
            join_set.spawn_blocking(move || {
                handle.block_on(
                    async move {
                        Self::run_benchmark_internal(
                            bps_share,
                            operations,
                            key_pair,
                            epoch,
                            chain_client,
                            shutdown_notifier,
                            sender,
                            committee,
                            local_node,
                            close_chains,
                        )
                        .await?;

                        Ok(())
                    }
                    .instrument(tracing::info_span!(
                        "benchmark_chain_id",
                        chain_id = short_chain_id
                    )),
                )
            });
        }

        join_set
            .join_all()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        drop(sender);
        info!("All benchmark tasks completed");
        bps_control_task.await?;

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_benchmark_internal(
        bps: Option<usize>,
        operations: Vec<Operation>,
        key_pair: AccountSecretKey,
        epoch: Epoch,
        chain_client: ChainClient<NodeProvider, S>,
        shutdown_notifier: CancellationToken,
        sender: crossbeam_channel::Sender<()>,
        committee: Committee,
        local_node: LocalNodeClient<S>,
        close_chains: bool,
    ) -> Result<(), Error> {
        let chain_id = chain_client.chain_id();
        info!(
            "Starting benchmark at target BPS of {:?}, for chain {:?}",
            bps, chain_id
        );
        let cross_chain_message_delivery = chain_client.options().cross_chain_message_delivery;
        let mut num_sent_proposals = 0;
        loop {
            if shutdown_notifier.is_cancelled() {
                info!("Shutdown signal received, stopping benchmark");
                break;
            }
            let block = ProposedBlock {
                epoch,
                chain_id,
                incoming_bundles: Vec::new(),
                operations: operations.clone(),
                previous_block_hash: chain_client.block_hash(),
                height: chain_client.next_block_height(),
                authenticated_signer: Some(Owner::from(key_pair.public())),
                timestamp: chain_client.timestamp().max(Timestamp::now()),
            };
            let executed_block = local_node
                .stage_block_execution(block.clone(), None)
                .await?
                .0;

            let value = Hashed::new(ConfirmedBlock::new(executed_block));
            let proposal =
                BlockProposal::new_initial(linera_base::data_types::Round::Fast, block, &key_pair);

            chain_client
                .submit_block_proposal(&committee, Box::new(proposal), value)
                .await?;
            let next_block_height = chain_client.next_block_height();
            // We assume the committee will not change during the benchmark.
            chain_client
                .communicate_chain_updates(
                    &committee,
                    chain_id,
                    next_block_height,
                    cross_chain_message_delivery,
                )
                .await?;

            num_sent_proposals += 1;
            if let Some(bps) = bps {
                if num_sent_proposals == bps {
                    sender.send(())?;
                    num_sent_proposals = 0;
                }
            } else {
                sender.send(())?;
                break;
            }
        }

        if close_chains {
            Self::close_benchmark_chain(chain_client).await?;
        }
        info!("Exiting task...");
        Ok(())
    }

    /// Closes the chain that was created for the benchmark.
    async fn close_benchmark_chain(
        chain_client: ChainClient<NodeProvider, S>,
    ) -> Result<(), Error> {
        let start = Instant::now();
        chain_client
            .execute_operation(Operation::System(SystemOperation::CloseChain))
            .await?
            .expect("Close chain operation should not fail!");

        debug!(
            "Closed chain {:?} in {} ms",
            chain_client.chain_id(),
            start.elapsed().as_millis()
        );

        Ok(())
    }

    /// Generates information related to one block per chain, up to `num_chains` blocks.
    pub fn make_benchmark_block_info(
        key_pairs: HashMap<ChainId, AccountSecretKey>,
        transactions_per_block: usize,
        fungible_application_id: Option<ApplicationId>,
    ) -> Vec<(ChainId, Vec<Operation>, AccountSecretKey)> {
        let mut blocks_infos = Vec::new();
        let mut previous_chain_id = *key_pairs
            .iter()
            .last()
            .expect("There should be a last element")
            .0;
        let amount = Amount::from(1);
        for (chain_id, key_pair) in key_pairs {
            let public_key = key_pair.public();
            let operation = match fungible_application_id {
                Some(application_id) => Self::fungible_transfer(
                    application_id,
                    previous_chain_id,
                    public_key,
                    public_key,
                    amount,
                ),
                None => Operation::System(SystemOperation::Transfer {
                    owner: None,
                    recipient: Recipient::chain(previous_chain_id),
                    amount,
                }),
            };
            let operations = iter::repeat(operation)
                .take(transactions_per_block)
                .collect();
            blocks_infos.push((chain_id, operations, key_pair));
            previous_chain_id = chain_id;
        }
        blocks_infos
    }

    /// Creates a fungible token transfer operation.
    pub fn fungible_transfer(
        application_id: ApplicationId,
        chain_id: ChainId,
        sender: AccountPublicKey,
        receiver: AccountPublicKey,
        amount: Amount,
    ) -> Operation {
        let target_account = fungible::Account {
            chain_id,
            owner: AccountOwner::User(Owner::from(receiver)),
        };
        let bytes = bcs::to_bytes(&fungible::Operation::Transfer {
            owner: AccountOwner::User(Owner::from(sender)),
            amount,
            target_account,
        })
        .expect("should serialize fungible token operation");
        Operation::User {
            application_id,
            bytes,
        }
    }
}
