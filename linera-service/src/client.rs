// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#![deny(warnings)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use linera_base::{
    committee::ValidatorState,
    crypto::{CryptoHash, KeyPair},
    data_types::{
        BlockHeight, ChainDescription, ChainId, Epoch, Owner, RoundNumber, Timestamp, ValidatorName,
    },
};
use linera_chain::data_types::{
    Block, BlockAndRound, BlockProposal, Certificate, SignatureAggregator, Value, Vote,
};
use linera_core::{
    client::{ChainClientState, ValidatorNodeProvider},
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{LocalNodeClient, ValidatorNode},
    worker::{Reason, WorkerState},
};
use linera_execution::{
    system::{Address, Amount, Balance, SystemOperation, UserData},
    ApplicationId, Operation,
};
use linera_rpc::{
    config::NetworkProtocol, grpc_network::GrpcMassClient, mass::MassClient,
    node_provider::NodeProvider, simple_network, RpcMessage,
};
use linera_service::{
    config::{CommitteeConfig, Export, GenesisConfig, Import, UserChain, WalletState},
    storage::{Runnable, StorageConfig},
    tracker::NotificationTracker,
};
use linera_storage::Store;
use linera_views::views::ViewError;
use log::{debug, error, info, warn};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    time::{Duration, Instant},
};
use structopt::StructOpt;

struct ClientContext {
    genesis_config: GenesisConfig,
    wallet_state_path: PathBuf,
    wallet_state: WalletState,
    max_pending_messages: usize,
    send_timeout: Duration,
    recv_timeout: Duration,
    cross_chain_delay: Duration,
    cross_chain_retries: usize,
}

impl ClientContext {
    fn from_options(options: &ClientOptions) -> Self {
        let wallet_state_path = options.wallet_state_path.clone();
        let wallet_state =
            WalletState::read_or_create(&wallet_state_path).expect("Unable to read user chains");
        let genesis_config = match options.command {
            ClientCommand::CreateGenesisConfig { admin_root, .. } => {
                GenesisConfig::new(CommitteeConfig::default(), ChainId::root(admin_root))
            }
            _ => GenesisConfig::read(&options.genesis_config_path)
                .expect("Fail to read initial chain config"),
        };
        let send_timeout = Duration::from_micros(options.send_timeout_us);
        let recv_timeout = Duration::from_micros(options.recv_timeout_us);
        let cross_chain_delay = Duration::from_micros(options.cross_chain_delay_ms);

        ClientContext {
            genesis_config,
            wallet_state_path,
            wallet_state,
            max_pending_messages: options.max_pending_messages,
            send_timeout,
            recv_timeout,
            cross_chain_delay,
            cross_chain_retries: options.cross_chain_retries,
        }
    }

    fn make_validator_mass_clients(&self, max_in_flight: u64) -> Vec<Box<dyn MassClient>> {
        let mut validator_clients = Vec::new();
        for config in &self.genesis_config.committee.validators {
            let client: Box<dyn MassClient> = match config.network.protocol {
                NetworkProtocol::Simple(protocol) => {
                    let network = config.network.clone_with_protocol(protocol);
                    Box::new(simple_network::SimpleMassClient::new(
                        network,
                        self.send_timeout,
                        self.recv_timeout,
                        max_in_flight,
                    ))
                }
                NetworkProtocol::Grpc => Box::new(GrpcMassClient::new(config.network.clone())),
            };

            validator_clients.push(client);
        }
        validator_clients
    }

    fn make_chain_client<S>(
        &self,
        storage: S,
        chain_id: ChainId,
    ) -> ChainClientState<impl ValidatorNodeProvider, S> {
        let chain = self.wallet_state.get(chain_id).expect("Unknown chain");
        let node_provider = NodeProvider::new(self.send_timeout, self.recv_timeout);
        ChainClientState::new(
            chain_id,
            chain
                .key_pair
                .as_ref()
                .map(|kp| kp.copy())
                .into_iter()
                .collect(),
            node_provider,
            storage,
            self.genesis_config.admin_id,
            self.max_pending_messages,
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            self.cross_chain_delay,
            self.cross_chain_retries,
        )
    }

    async fn process_inboxes_and_force_validator_updates<S>(&mut self, storage: &S)
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for chain_id in self.wallet_state.chain_ids() {
            let mut client = self.make_chain_client(storage.clone(), chain_id);
            client.process_inbox().await.unwrap();
            client.update_validators_about_local_chain().await.unwrap();
            self.update_wallet_from_client(&mut client).await;
        }
    }

    /// Make one block proposal per chain, up to `max_proposals` blocks.
    fn make_benchmark_block_proposals(&mut self, max_proposals: usize) -> Vec<RpcMessage> {
        let mut proposals = Vec::new();
        let mut next_recipient = self.wallet_state.last_chain().unwrap().chain_id;
        for chain in self.wallet_state.chains_mut() {
            let key_pair = match &chain.key_pair {
                Some(kp) => kp,
                None => continue,
            };
            let block = Block {
                epoch: Epoch::from(0),
                chain_id: chain.chain_id,
                incoming_messages: Vec::new(),
                operations: vec![(
                    ApplicationId::System,
                    Operation::System(SystemOperation::Transfer {
                        recipient: Address::Account(next_recipient),
                        amount: Amount::from(1),
                        user_data: UserData::default(),
                    }),
                )],
                previous_block_hash: chain.block_hash,
                height: chain.next_block_height,
                timestamp: chain.timestamp.max(Timestamp::now()),
            };
            debug!("Preparing block proposal: {:?}", block);
            let proposal = BlockProposal::new(
                BlockAndRound {
                    block: block.clone(),
                    round: RoundNumber::default(),
                },
                key_pair,
            );
            proposals.push(proposal.into());
            if proposals.len() >= max_proposals {
                break;
            }
            next_recipient = chain.chain_id;
        }
        proposals
    }

    /// Try to aggregate votes into certificates.
    fn make_benchmark_certificates_from_votes(
        &self,
        votes: Vec<(Vote, Value)>,
    ) -> Vec<Certificate> {
        let committee = self.genesis_config.committee.clone().into_committee();
        let mut aggregators = HashMap::new();
        let mut certificates = Vec::new();
        let mut done_senders = HashSet::new();
        for (vote, value) in votes {
            // We aggregate votes indexed by sender.
            let chain_id = value.chain_id();
            if done_senders.contains(&chain_id) {
                continue;
            }
            debug!(
                "Processing vote on {:?}'s block by {:?}",
                chain_id, vote.validator,
            );
            let aggregator = aggregators
                .entry(chain_id)
                .or_insert_with(|| SignatureAggregator::new(value, &committee));
            match aggregator.append(vote.validator, vote.signature) {
                Ok(Some(certificate)) => {
                    debug!("Found certificate: {:?}", certificate);
                    certificates.push(certificate);
                    done_senders.insert(chain_id);
                }
                Ok(None) => {
                    debug!("Added one vote");
                }
                Err(error) => {
                    error!("Failed to aggregate vote: {}", error);
                }
            }
        }
        certificates
    }

    /// Broadcast a bulk of blocks to each validator.
    async fn mass_broadcast(
        &self,
        phase: &'static str,
        max_in_flight: u64,
        proposals: Vec<RpcMessage>,
    ) -> Vec<RpcMessage> {
        let time_start = Instant::now();
        info!("Broadcasting {} {}", proposals.len(), phase);
        let mut handles = Vec::new();
        for client in self.make_validator_mass_clients(max_in_flight) {
            let proposals = proposals.clone();
            handles.push(tokio::spawn(async move {
                info!("Sending {} requests", proposals.len());
                let responses = client.send(proposals).await.unwrap_or_default();
                info!("Done sending requests");
                responses
            }));
        }
        let responses = futures::future::join_all(handles)
            .await
            .into_iter()
            .flatten()
            .flatten()
            .collect::<Vec<RpcMessage>>();
        let time_elapsed = time_start.elapsed();
        warn!(
            "Received {} responses in {} ms.",
            responses.len(),
            time_elapsed.as_millis()
        );
        warn!(
            "Estimated server throughput: {} {} per sec",
            (proposals.len() as u128) * 1_000_000 / time_elapsed.as_micros(),
            phase
        );
        responses
    }

    fn save_chains(&self) {
        self.wallet_state
            .write(&self.wallet_state_path)
            .expect("Unable to write user chains");
        info!("Saved user chain states");
    }

    async fn update_wallet_from_client<P, S>(&mut self, state: &mut ChainClientState<P, S>)
    where
        P: ValidatorNodeProvider + Send + 'static,
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        self.wallet_state.update_from_state(state).await
    }

    /// Remember the new private key (if any) in the wallet.
    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) {
        self.wallet_state.insert(UserChain {
            chain_id,
            key_pair: key_pair.as_ref().map(|kp| kp.copy()),
            block_hash: None,
            timestamp,
            next_block_height: BlockHeight::from(0),
        });
    }

    async fn update_wallet_from_certificates<S>(
        &mut self,
        storage: S,
        certificates: Vec<Certificate>,
    ) where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        // First instantiate a local node on top of storage.
        let worker = WorkerState::new("Temporary client node".to_string(), None, storage)
            .with_allow_inactive_chains(true)
            .with_allow_messages_from_deprecated_epochs(true);
        let mut node = LocalNodeClient::new(worker);
        // Second replay the certificates locally.
        for certificate in certificates {
            node.handle_certificate(certificate).await.unwrap();
        }
        // Last update the wallet.
        for chain in self.wallet_state.chains_mut() {
            let query = ChainInfoQuery::new(chain.chain_id);
            let info = node.handle_chain_info_query(query).await.unwrap().info;
            // We don't have private keys but that's ok.
            chain.block_hash = info.block_hash;
            chain.next_block_height = info.next_block_height;
        }
    }

    async fn ensure_admin_subscription<S>(&mut self, storage: &S) -> Vec<Certificate>
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let mut certificates = Vec::new();
        for chain_id in self.wallet_state.chain_ids() {
            let mut client_state = self.make_chain_client(storage.clone(), chain_id);
            if let Ok(cert) = client_state.subscribe_to_new_committees().await {
                info!(
                    "Subscribed {:?} to the admin chain {:?}",
                    chain_id, self.genesis_config.admin_id,
                );
                certificates.push(cert);
                self.update_wallet_from_client(&mut client_state).await;
            }
        }
        certificates
    }

    async fn push_to_all_chains<S>(&mut self, storage: &S, certificate: &Certificate)
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for chain_id in self.wallet_state.chain_ids() {
            info!("pushing certificate to {:?}", chain_id);
            let mut client_state = self.make_chain_client(storage.clone(), chain_id);
            client_state
                .receive_certificate(certificate.clone())
                .await
                .unwrap();
            client_state.process_inbox().await.unwrap();
            let epochs = client_state.epochs().await.unwrap();
            info!("{:?} accepts epochs {:?}", chain_id, epochs);
            self.update_wallet_from_client(&mut client_state).await;
        }
    }
}

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

#[derive(StructOpt)]
#[structopt(
    name = "Linera Client",
    about = "A Byzantine-fault tolerant sidechain with low-latency finality and high throughput"
)]
struct ClientOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
    #[structopt(long = "wallet")]
    wallet_state_path: PathBuf,

    /// Storage configuration for the blockchain history.
    #[structopt(long = "storage", default_value = "memory")]
    storage_config: StorageConfig,

    /// Optional path to the file describing the initial user chains (aka genesis state)
    #[structopt(long = "genesis")]
    genesis_config_path: PathBuf,

    /// Timeout for sending queries (us)
    #[structopt(long, default_value = "4000000")]
    send_timeout_us: u64,

    /// Timeout for receiving responses (us)
    #[structopt(long, default_value = "4000000")]
    recv_timeout_us: u64,

    /// Time between attempts while waiting on cross-chain updates (ms)
    #[structopt(long, default_value = "4000")]
    cross_chain_delay_ms: u64,

    #[structopt(long, default_value = "10")]
    cross_chain_retries: usize,

    #[structopt(long, default_value = "10")]
    max_pending_messages: usize,

    /// Subcommand.
    #[structopt(subcommand)]
    command: ClientCommand,
}

#[derive(StructOpt)]
enum ClientCommand {
    /// Transfer funds
    #[structopt(name = "transfer")]
    Transfer {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,

        /// Recipient chain id
        #[structopt(long = "to")]
        recipient: ChainId,

        /// Amount to transfer
        amount: Amount,
    },

    /// Open (i.e. activate) a new chain deriving the UID from an existing one.
    #[structopt(name = "open_chain")]
    OpenChain {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,

        /// Public key of the new owner (otherwise create a key pair and remember it)
        #[structopt(long = "to-owner")]
        owner: Option<Owner>,
    },

    /// Close (i.e. deactivate) an existing chain. (Consider `spend_and_transfer`
    /// instead for real-life use cases.)
    #[structopt(name = "close_chain")]
    CloseChain {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,
    },

    /// Read the balance of the chain from the local state of the client.
    #[structopt(name = "query_balance")]
    QueryBalance {
        /// Chain id
        chain_id: ChainId,
    },

    /// Synchronize the local state of the chain (including a conservative estimation of the
    /// available balance) with a quorum validators.
    #[structopt(name = "sync_balance")]
    SynchronizeBalance {
        /// Chain id
        chain_id: ChainId,
    },

    /// Show the current set of validators for a chain.
    #[structopt(name = "query_validators")]
    QueryValidators {
        /// Chain id (defaults to admin chain)
        chain_id: Option<ChainId>,
    },

    /// Add or modify a validator (admin only)
    #[structopt(name = "set_validator")]
    SetValidator {
        /// The public key of the validator.
        #[structopt(long = "name")]
        name: ValidatorName,

        /// Address
        #[structopt(long = "address")]
        network_address: String,

        /// Voting power
        #[structopt(long = "votes", default_value = "1")]
        votes: u64,
    },

    /// Remove a validator (admin only)
    #[structopt(name = "remove_validator")]
    RemoveValidator {
        /// The public key of the validator.
        #[structopt(long = "name")]
        name: ValidatorName,
    },

    /// Send one transfer per chain in bulk mode
    #[structopt(name = "benchmark")]
    Benchmark {
        /// Maximum number of blocks in flight
        #[structopt(long, default_value = "200")]
        max_in_flight: u64,

        /// Use a subset of the chains to generate N transfers
        #[structopt(long)]
        max_proposals: Option<usize>,
    },

    /// Create initial user chains and print information to be used for initialization of validator setup.
    #[structopt(name = "create_genesis_config")]
    CreateGenesisConfig {
        /// Sets the file describing the public configurations of all validators
        #[structopt(long = "committee")]
        committee_config_path: PathBuf,

        /// Index of the admin chain in the genesis config
        #[structopt(long, default_value = "0")]
        admin_root: usize,

        /// Known initial balance of the chain
        #[structopt(long, default_value = "0")]
        initial_funding: Balance,

        /// The start timestamp: no blocks can be created before this time.
        #[structopt(long)]
        start_timestamp: Option<DateTime<Utc>>,

        /// Number of additional chains to create
        num: u32,
    },

    /// Watch the network for notifications.
    #[structopt(name = "watch")]
    Watch {
        /// The collection of Chain IDs to watch.
        chain_ids: Vec<ChainId>,

        /// Show all notifications from all validators.
        #[structopt(long)]
        raw: bool,
    },

    /// Acts as a Synchronizer on the network.
    #[structopt(name = "synchronize")]
    Synchronize {
        /// The collection of Chain IDs to synchronize for.
        chain_ids: Vec<ChainId>,
    },
}

struct Job(ClientContext, ClientCommand);

#[async_trait]
impl<S> Runnable<S> for Job
where
    S: Store + Clone + Send + Sync + 'static,
    ViewError: From<S::ContextError>,
{
    type Output = ();

    async fn run(self, storage: S) -> Result<(), anyhow::Error> {
        let Job(mut context, command) = self;
        use ClientCommand::*;
        match command {
            Transfer {
                sender,
                recipient,
                amount,
            } => {
                let mut client_state = context.make_chain_client(storage, sender);
                info!("Starting transfer");
                let time_start = Instant::now();
                let certificate = client_state
                    .transfer_to_chain(amount, recipient, UserData::default())
                    .await
                    .unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                info!("{:?}", certificate);
                context.update_wallet_from_client(&mut client_state).await;
                context.save_chains();
            }

            OpenChain { sender, owner } => {
                let mut client_state = context.make_chain_client(storage, sender);
                let (new_owner, key_pair) = match owner {
                    Some(key) => (key, None),
                    None => {
                        let key_pair = KeyPair::generate();
                        (Owner(key_pair.public()), Some(key_pair))
                    }
                };
                info!("Starting operation to open a new chain");
                let time_start = Instant::now();
                let (id, certificate) = client_state.open_chain(new_owner).await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                info!("{:?}", certificate);
                context.update_wallet_from_client(&mut client_state).await;
                let timestamp = certificate.value.block().timestamp;
                context.update_wallet_for_new_chain(id, key_pair, timestamp);
                // Print the new chain id(s) on stdout for the scripting purposes.
                println!("{}", id);
                context.save_chains();
            }

            CloseChain { sender } => {
                let mut client_state = context.make_chain_client(storage, sender);
                info!("Starting operation to close the chain");
                let time_start = Instant::now();
                let certificate = client_state.close_chain().await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                info!("{:?}", certificate);
                context.update_wallet_from_client(&mut client_state).await;
                context.save_chains();
            }

            QueryBalance { chain_id } => {
                let mut client_state = context.make_chain_client(storage, chain_id);
                info!("Starting query for the local balance");
                let time_start = Instant::now();
                let balance = client_state
                    .local_balance()
                    .await
                    .expect("Use sync_balance instead");
                let time_total = time_start.elapsed().as_micros();
                info!("Local balance obtained after {} us", time_total);
                println!("{}", balance);
                context.update_wallet_from_client(&mut client_state).await;
                context.save_chains();
            }

            SynchronizeBalance { chain_id } => {
                let mut client_state = context.make_chain_client(storage, chain_id);
                info!("Synchronize chain information");
                let time_start = Instant::now();
                let balance = client_state
                    .synchronize_and_recompute_balance()
                    .await
                    .unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Chain balance synchronized after {} us", time_total);
                println!("{}", balance);
                context.update_wallet_from_client(&mut client_state).await;
                context.save_chains();
            }

            QueryValidators { chain_id } => {
                let mut client_state = context.make_chain_client(
                    storage,
                    chain_id.unwrap_or(context.genesis_config.admin_id),
                );
                info!("Starting operation to query validators");
                let time_start = Instant::now();
                let committee = client_state.local_committee().await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Validators obtained after {} us", time_total);
                info!("{:?}", committee.validators);
                context.update_wallet_from_client(&mut client_state).await;
                context.save_chains();
            }

            command @ (SetValidator { .. } | RemoveValidator { .. }) => {
                info!("Starting operations to change validator set");
                let time_start = Instant::now();

                // Make sure genesis chains are subscribed to the admin chain.
                let certificates = context.ensure_admin_subscription(&storage).await;
                let mut admin_state =
                    context.make_chain_client(storage.clone(), context.genesis_config.admin_id);
                for cert in certificates {
                    admin_state.receive_certificate(cert).await.unwrap();
                }
                let n = admin_state
                    .process_inbox()
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|c| c.value.effects_and_state_hash().0.len())
                    .sum::<usize>();
                log::info!("Subscribed {} chains to new committees", n);

                // Create the new committee.
                let committee = admin_state.local_committee().await.unwrap();
                let mut validators = committee.validators;
                match command {
                    SetValidator {
                        name,
                        network_address,
                        votes,
                    } => {
                        validators.insert(
                            name,
                            ValidatorState {
                                network_address,
                                votes,
                            },
                        );
                    }
                    RemoveValidator { name } => {
                        if validators.remove(&name).is_none() {
                            warn!("Skipping removal of nonexistent validator");
                            return Ok(());
                        }
                    }
                    _ => unreachable!(),
                }
                let certificate = admin_state.stage_new_committee(validators).await.unwrap();
                context.update_wallet_from_client(&mut admin_state).await;
                info!("Staging committee:\n{:?}", certificate);
                context.push_to_all_chains(&storage, &certificate).await;

                // Remove the old committee.
                let certificate = admin_state.finalize_committee().await.unwrap();
                context.update_wallet_from_client(&mut admin_state).await;
                info!("Finalizing committee:\n{:?}", certificate);
                context.push_to_all_chains(&storage, &certificate).await;

                let time_total = time_start.elapsed().as_micros();
                info!("Operations confirmed after {} us", time_total);
                context.save_chains();
            }

            Benchmark {
                max_in_flight,
                max_proposals,
            } => {
                // Below all block proposals are supposed to succeed without retries, we
                // must make sure that all incoming payments have been accepted on-chain
                // and that no validator is missing user certificates.
                context
                    .process_inboxes_and_force_validator_updates(&storage)
                    .await;

                // For this command, we create proposals and gather certificates without using
                // the client library. We update the wallet storage at the end using a local node.
                let max_proposals =
                    max_proposals.unwrap_or_else(|| context.wallet_state.num_chains());
                warn!("Starting benchmark phase 1 (block proposals)");
                let proposals = context.make_benchmark_block_proposals(max_proposals);
                let num_proposal = proposals.len();
                let mut values = HashMap::new();

                for rpc_msg in &proposals {
                    if let RpcMessage::BlockProposal(proposal) = rpc_msg {
                        let block = proposal.content.block.clone();
                        let (effects, response) =
                            WorkerState::new("staging".to_string(), None, storage.clone())
                                .stage_block_execution(&block)
                                .await?;
                        let state_hash = response.info.state_hash.expect("state was just computed");
                        let value = Value::ConfirmedBlock {
                            block,
                            effects,
                            state_hash,
                        };
                        values.insert(CryptoHash::new(&value), value);
                    }
                }

                let responses = context
                    .mass_broadcast("block proposals", max_in_flight, proposals)
                    .await;
                let votes: Vec<_> = responses
                    .into_iter()
                    .filter_map(|message| {
                        deserialize_response(message).and_then(|response| {
                            response.info.manager.pending().and_then(|vote| {
                                Some((vote.clone(), values.get(&vote.hash)?.clone()))
                            })
                        })
                    })
                    .collect();
                warn!("Received {} valid votes.", votes.len());

                warn!("Starting benchmark phase 2 (certified blocks)");
                let certificates = context.make_benchmark_certificates_from_votes(votes);
                assert_eq!(
                    num_proposal,
                    certificates.len(),
                    "Unable to build all the expected certificates from received votes"
                );
                let messages = certificates
                    .iter()
                    .map(|certificate| certificate.clone().into())
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
                warn!(
                    "Confirmed {} valid certificates for {} block proposals.",
                    num_valid,
                    confirmed.len()
                );

                warn!("Updating local state of user chains");
                context
                    .update_wallet_from_certificates(storage, certificates)
                    .await;
                context.save_chains();
            }

            Watch { chain_ids, raw } => {
                debug!("Watching for notifications for chains: {:?}", &chain_ids);
                let chain_id = match chain_ids.get(0) {
                    None => {
                        warn!("No chains specified, exiting...");
                        return Ok(());
                    }
                    Some(chain_id) => chain_id,
                };
                let mut client_state = context.make_chain_client(storage, *chain_id);
                let mut tracker = NotificationTracker::default();
                let mut notification_stream = client_state.subscribe_all(chain_ids).await?;
                while let Some(notification) = notification_stream.next().await {
                    if raw {
                        info!("{:?}", notification);
                    } else if tracker.insert(notification.clone()) {
                        info!("{:?}", notification);
                    }
                }
                warn!("Notification stream ended.")
            }

            Synchronize { chain_ids } => {
                debug!("Synchronizing chains: {:#?}", &chain_ids);
                let chain_id = match chain_ids.get(0) {
                    None => {
                        warn!("No chains specified, exiting...");
                        return Ok(());
                    }
                    Some(chain_id) => chain_id,
                };
                let mut notification_stream = context
                    .make_chain_client(storage.clone(), *chain_id)
                    .subscribe_all(chain_ids)
                    .await?;
                let mut tracker = NotificationTracker::default();
                while let Some(notification) = notification_stream.next().await {
                    debug!("Received notification: {:?}", notification);
                    let mut client =
                        context.make_chain_client(storage.clone(), notification.chain_id);
                    if tracker.insert(notification.clone()) {
                        if let Err(e) = client.synchronize_and_recompute_balance().await {
                            warn!("Failed to synchronize and recompute balance for notification {:?} with error: {:?}", notification, e);
                            // If synchronization failed there is nothing to update validators about.
                            continue;
                        }
                        match &notification.reason {
                            Reason::NewBlock { .. } => {
                                if let Err(e) = client.update_validators_about_local_chain().await {
                                    warn!("Failed to update validators about the local chain after receiving notification {:?} with error: {:?}", notification, e);
                                }
                            }
                            Reason::NewMessage { .. } => {
                                if let Err(e) = client.process_inbox().await {
                                    warn!("Failed to process inbox after receiving new message: {:?} with error: {:?}", notification, e);
                                }
                            }
                        }
                    }
                }
                warn!("Notification stream ended.")
            }

            CreateGenesisConfig { .. } => unreachable!(),
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    let options = ClientOptions::from_args();
    let mut context = ClientContext::from_options(&options);
    match options.command {
        ClientCommand::CreateGenesisConfig {
            committee_config_path,
            admin_root,
            initial_funding,
            start_timestamp,
            num,
        } => {
            let committee_config = CommitteeConfig::read(&committee_config_path)
                .expect("Unable to read committee config file");
            let mut genesis_config =
                GenesisConfig::new(committee_config, ChainId::root(admin_root));
            let timestamp = start_timestamp
                .map(|st| Timestamp::from(st.timestamp() as u64))
                .unwrap_or_default();
            for i in 0..num {
                let description = ChainDescription::Root(i as usize);
                // Create keys.
                let chain = UserChain::make_initial(description, timestamp);
                // Public "genesis" state.
                genesis_config.chains.push((
                    description,
                    Owner(chain.key_pair.as_ref().unwrap().public()),
                    initial_funding,
                    timestamp,
                ));
                // Private keys.
                context.wallet_state.insert(chain);
            }
            context.save_chains();
            genesis_config.write(&options.genesis_config_path).unwrap();
        }
        command => {
            let genesis_config = context.genesis_config.clone();
            options
                .storage_config
                .run_with_storage(&genesis_config, Job(context, command))
                .await
                .unwrap()
        }
    }
}
