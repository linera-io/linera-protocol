// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, bail, Context, Error};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use colored::Colorize;
use futures::{lock::Mutex, StreamExt};
use linera_base::{
    crypto::{CryptoHash, CryptoRng, KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{BytecodeId, ChainDescription, ChainId, MessageId, Owner},
};
use linera_chain::data_types::{Certificate, CertificateValue, ExecutedBlock};
use linera_core::{
    client::{ChainClient, ChainClientBuilder},
    data_types::ChainInfoQuery,
    local_node::LocalNodeClient,
    node::ValidatorNodeProvider,
    notifier::Notifier,
    tracker::NotificationTracker,
    worker::WorkerState,
};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    policy::ResourceControlPolicy,
    system::{Account, UserData},
    Bytecode, ChainOwnership, Message, SystemMessage, UserApplicationId, WasmRuntime,
    WithWasmDefault,
};
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
#[cfg(feature = "kubernetes")]
use linera_service::cli_wrappers::local_kubernetes_net::LocalKubernetesNetConfig;
use linera_service::{
    chain_listener::{self, ChainListenerConfig},
    cli_wrappers::{
        self,
        local_net::{Database, LocalNetConfig},
        ClientWrapper, LineraNet, LineraNetConfig, Network,
    },
    config::{CommitteeConfig, Export, GenesisConfig, Import, UserChain, WalletState},
    faucet::FaucetService,
    node_service::NodeService,
    project::{self, Project},
    storage::{full_initialize_storage, run_with_storage, Runnable, StorageConfig},
};
use linera_storage::Store;
use linera_views::{common::CommonStoreConfig, views::ViewError};
use rand07::Rng;
use serde_json::Value;
use std::{
    collections::HashMap,
    env, fs, iter,
    num::NonZeroU16,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tokio::signal::unix;
use tracing::{debug, info, warn};

#[cfg(feature = "benchmark")]
use {
    linera_chain::data_types::{
        Block, BlockAndRound, BlockProposal, HashedValue, SignatureAggregator, Vote,
    },
    linera_core::data_types::ChainInfoResponse,
    linera_execution::{
        committee::Epoch,
        system::{Recipient, SystemOperation},
        Operation,
    },
    linera_rpc::{
        config::NetworkProtocol, grpc_network::GrpcClient, mass::MassClient, simple_network,
        HandleCertificateRequest, RpcMessage,
    },
    std::collections::HashSet,
    tracing::{error, trace},
};

struct ClientContext {
    wallet_state: WalletState,
    chain_client_builder: ChainClientBuilder<NodeProvider>,
    send_timeout: Duration,
    recv_timeout: Duration,
    notification_retry_delay: Duration,
    notification_retries: u32,
    wait_for_outgoing_messages: bool,
    prng: Box<dyn CryptoRng>,
}

#[async_trait]
impl chain_listener::ClientContext<NodeProvider> for ClientContext {
    fn wallet_state(&self) -> &WalletState {
        &self.wallet_state
    }

    fn make_chain_client<S>(
        &self,
        storage: S,
        chain_id: impl Into<Option<ChainId>>,
    ) -> ChainClient<NodeProvider, S> {
        self.make_chain_client(storage, chain_id)
    }

    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) {
        self.update_wallet_for_new_chain(chain_id, key_pair, timestamp);
    }

    async fn update_wallet<'a, S>(&'a mut self, client: &'a mut ChainClient<NodeProvider, S>)
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        self.update_and_save_wallet(client).await;
    }
}

impl ClientContext {
    fn create(
        options: &ClientOptions,
        genesis_config: GenesisConfig,
        testing_prng_seed: Option<u64>,
        chains: Vec<UserChain>,
    ) -> Result<Self, anyhow::Error> {
        let wallet_state_path = match &options.wallet_state_path {
            Some(path) => path.clone(),
            None => Self::create_default_wallet_path()?,
        };
        anyhow::ensure!(
            !wallet_state_path.exists(),
            "Wallet already exists at {}. Aborting...",
            wallet_state_path.display()
        );
        let mut wallet_state =
            WalletState::create(&wallet_state_path, genesis_config, testing_prng_seed)
                .with_context(|| format!("Unable to create wallet at {:?}", &wallet_state_path))?;
        chains
            .into_iter()
            .for_each(|chain| wallet_state.insert(chain));
        Ok(Self::configure(options, wallet_state))
    }

    fn from_options(options: &ClientOptions) -> Result<Self, anyhow::Error> {
        let wallet_state_path = match &options.wallet_state_path {
            Some(path) => path.clone(),
            None => Self::create_default_wallet_path()?,
        };
        let wallet_state = WalletState::from_file(&wallet_state_path)?;
        Ok(Self::configure(options, wallet_state))
    }

    fn configure(options: &ClientOptions, wallet_state: WalletState) -> Self {
        let send_timeout = Duration::from_micros(options.send_timeout_us);
        let recv_timeout = Duration::from_micros(options.recv_timeout_us);
        let cross_chain_delay = Duration::from_micros(options.cross_chain_delay_ms);
        let notification_retry_delay = Duration::from_micros(options.notification_retry_delay_us);
        let prng = wallet_state.make_prng();

        let node_options = NodeOptions {
            send_timeout,
            recv_timeout,
            notification_retry_delay,
            notification_retries: options.notification_retries,
            wait_for_outgoing_messages: options.wait_for_outgoing_messages,
        };
        let node_provider = NodeProvider::new(node_options);
        let chain_client_builder = ChainClientBuilder::new(
            node_provider,
            options.max_pending_messages,
            cross_chain_delay,
            options.cross_chain_retries,
        );
        ClientContext {
            chain_client_builder,
            wallet_state,
            send_timeout,
            recv_timeout,
            notification_retry_delay,
            notification_retries: options.notification_retries,
            wait_for_outgoing_messages: options.wait_for_outgoing_messages,
            prng,
        }
    }

    fn create_default_config_path() -> Result<PathBuf, anyhow::Error> {
        let mut config_dir = dirs::config_dir()
            .context("Default configuration directory not supported. Please specify a path.")?;
        config_dir.push("linera");
        if !config_dir.exists() {
            debug!("{} does not exist, creating...", config_dir.display());
            fs::create_dir(&config_dir)?;
            debug!("{} created.", config_dir.display());
        }
        Ok(config_dir)
    }

    fn create_default_wallet_path() -> Result<PathBuf, anyhow::Error> {
        Ok(Self::create_default_config_path()?.join("wallet.json"))
    }

    fn storage_config(options: &ClientOptions) -> Result<StorageConfig, anyhow::Error> {
        match &options.storage_config {
            Some(config) => config.parse(),
            #[cfg(feature = "rocksdb")]
            None => Ok(StorageConfig::RocksDb {
                path: Self::create_default_config_path()?.join("wallet.db"),
            }),
            #[cfg(not(feature = "rocksdb"))]
            None => bail!("A storage option must be provided"),
        }
    }

    #[cfg(feature = "benchmark")]
    fn make_validator_mass_clients(&self, max_in_flight: u64) -> Vec<Box<dyn MassClient>> {
        let mut validator_clients = Vec::new();
        for config in &self.wallet_state.genesis_config().committee.validators {
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
                NetworkProtocol::Grpc { .. } => Box::new(
                    GrpcClient::new(config.network.clone(), self.make_node_options()).unwrap(),
                ),
            };

            validator_clients.push(client);
        }
        validator_clients
    }

    fn make_chain_client<S>(
        &self,
        storage: S,
        chain_id: impl Into<Option<ChainId>>,
    ) -> ChainClient<NodeProvider, S> {
        let chain_id = chain_id.into().unwrap_or_else(|| {
            self.wallet_state
                .default_chain()
                .expect("No chain specified in wallet with no default chain")
        });
        let chain = self
            .wallet_state
            .get(chain_id)
            .unwrap_or_else(|| panic!("Unknown chain: {}", chain_id));
        let known_key_pairs = chain
            .key_pair
            .as_ref()
            .map(|kp| kp.copy())
            .into_iter()
            .collect();
        self.chain_client_builder.build(
            chain_id,
            known_key_pairs,
            storage,
            self.wallet_state.genesis_admin_chain(),
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            chain.pending_block.clone(),
        )
    }

    fn make_node_provider(&self) -> NodeProvider {
        NodeProvider::new(self.make_node_options())
    }

    fn make_node_options(&self) -> NodeOptions {
        NodeOptions {
            send_timeout: self.send_timeout,
            recv_timeout: self.recv_timeout,
            notification_retry_delay: self.notification_retry_delay,
            notification_retries: self.notification_retries,
            wait_for_outgoing_messages: self.wait_for_outgoing_messages,
        }
    }

    #[cfg(feature = "benchmark")]
    async fn process_inboxes_and_force_validator_updates<S>(&mut self, storage: &S)
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for chain_id in self.wallet_state.own_chain_ids() {
            let mut chain_client = self.make_chain_client(storage.clone(), chain_id);
            chain_client.process_inbox().await.unwrap();
            chain_client.update_validators().await.unwrap();
            self.update_wallet_from_client(&mut chain_client).await;
        }
    }

    /// Makes one block proposal per chain, up to `max_proposals` blocks.
    #[cfg(feature = "benchmark")]
    fn make_benchmark_block_proposals(&mut self, max_proposals: usize) -> Vec<RpcMessage> {
        let mut proposals = Vec::new();
        let mut next_recipient = self.wallet_state.last_chain().unwrap().chain_id;
        for chain in self.wallet_state.chains_mut() {
            let key_pair = match &chain.key_pair {
                Some(kp) => kp,
                None => continue,
            };
            let block = Block {
                epoch: Epoch::ZERO,
                chain_id: chain.chain_id,
                incoming_messages: Vec::new(),
                operations: vec![Operation::System(SystemOperation::Transfer {
                    owner: None,
                    recipient: Recipient::chain(next_recipient),
                    amount: Amount::ONE,
                    user_data: UserData::default(),
                })],
                previous_block_hash: chain.block_hash,
                height: chain.next_block_height,
                authenticated_signer: None,
                timestamp: chain.timestamp.max(Timestamp::now()),
            };
            trace!("Preparing block proposal: {:?}", block);
            let proposal = BlockProposal::new(
                BlockAndRound {
                    block: block.clone(),
                    round: linera_base::data_types::Round::Fast,
                },
                key_pair,
                vec![],
                None,
            );
            proposals.push(proposal.into());
            if proposals.len() >= max_proposals {
                break;
            }
            next_recipient = chain.chain_id;
        }
        proposals
    }

    /// Tries to aggregate votes into certificates.
    #[cfg(feature = "benchmark")]
    fn make_benchmark_certificates_from_votes(&self, votes: Vec<Vote>) -> Vec<Certificate> {
        let committee = self.wallet_state.genesis_config().create_committee();
        let mut aggregators = HashMap::new();
        let mut certificates = Vec::new();
        let mut done_senders = HashSet::new();
        for vote in votes {
            // We aggregate votes indexed by sender.
            let chain_id = vote.value().chain_id();
            if done_senders.contains(&chain_id) {
                continue;
            }
            trace!(
                "Processing vote on {:?}'s block by {:?}",
                chain_id,
                vote.validator,
            );
            let aggregator = aggregators.entry(chain_id).or_insert_with(|| {
                SignatureAggregator::new(
                    vote.value,
                    linera_base::data_types::Round::Fast,
                    &committee,
                )
            });
            match aggregator.append(vote.validator, vote.signature) {
                Ok(Some(certificate)) => {
                    trace!("Found certificate: {:?}", certificate);
                    certificates.push(certificate);
                    done_senders.insert(chain_id);
                }
                Ok(None) => {
                    trace!("Added one vote");
                }
                Err(error) => {
                    error!("Failed to aggregate vote: {}", error);
                }
            }
        }
        certificates
    }

    /// Broadcasts a bulk of blocks to each validator.
    #[cfg(feature = "benchmark")]
    async fn mass_broadcast(
        &self,
        phase: &'static str,
        max_in_flight: u64,
        proposals: Vec<RpcMessage>,
    ) -> Vec<RpcMessage> {
        let time_start = Instant::now();
        info!("Broadcasting {} {}", proposals.len(), phase);
        let mut handles = Vec::new();
        for mut client in self.make_validator_mass_clients(max_in_flight) {
            let proposals = proposals.clone();
            handles.push(tokio::spawn(async move {
                debug!("Sending {} requests", proposals.len());
                let responses = client.send(proposals).await.unwrap_or_default();
                debug!("Done sending requests");
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
        info!(
            "Received {} responses in {} ms.",
            responses.len(),
            time_elapsed.as_millis()
        );
        info!(
            "Estimated server throughput: {} {} per sec",
            (proposals.len() as u128) * 1_000_000 / time_elapsed.as_micros(),
            phase
        );
        responses
    }

    fn save_wallet(&mut self) {
        self.wallet_state.refresh_prng_seed(&mut self.prng);
        self.wallet_state
            .write()
            .expect("Unable to write user chains");
        info!("Saved user chain states");
    }

    async fn update_wallet_from_client<P, S>(&mut self, state: &mut ChainClient<P, S>)
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        self.wallet_state.update_from_state(state).await
    }

    async fn update_and_save_wallet<P, S>(&mut self, state: &mut ChainClient<P, S>)
    where
        P: ValidatorNodeProvider + Sync + 'static,
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        self.update_wallet_from_client(state).await;
        self.save_wallet()
    }

    /// Remembers the new private key (if any) in the wallet.
    fn update_wallet_for_new_chain(
        &mut self,
        chain_id: ChainId,
        key_pair: Option<KeyPair>,
        timestamp: Timestamp,
    ) {
        if self.wallet_state.get(chain_id).is_none() {
            self.wallet_state.insert(UserChain {
                chain_id,
                key_pair: key_pair.as_ref().map(|kp| kp.copy()),
                block_hash: None,
                timestamp,
                next_block_height: BlockHeight::ZERO,
                pending_block: None,
            });
        }
    }

    #[cfg(feature = "benchmark")]
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
        let mut node = LocalNodeClient::new(worker, Notifier::default());
        // Second replay the certificates locally.
        for certificate in certificates {
            // No required certificates from other chains: This is only used with benchmark.
            node.handle_certificate(certificate, vec![]).await.unwrap();
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
            let mut chain_client = self.make_chain_client(storage.clone(), chain_id);
            if let Ok(cert) = chain_client.subscribe_to_new_committees().await {
                debug!(
                    "Subscribed {:?} to the admin chain {:?}",
                    chain_id,
                    self.wallet_state.genesis_admin_chain(),
                );
                certificates.push(cert);
                self.update_wallet_from_client(&mut chain_client).await;
            }
        }
        certificates
    }

    async fn push_to_all_chains<S>(&mut self, storage: &S, certificate: &Certificate)
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        for chain_id in self.wallet_state.own_chain_ids() {
            let mut chain_client = self.make_chain_client(storage.clone(), chain_id);
            chain_client
                .receive_certificate(certificate.clone())
                .await
                .unwrap();
            chain_client.process_inbox().await.unwrap();
            let epochs = chain_client.epochs().await.unwrap();
            debug!("{:?} accepts epochs {:?}", chain_id, epochs);
            self.update_wallet_from_client(&mut chain_client).await;
        }
    }

    async fn publish_bytecode<S>(
        &self,
        chain_client: &mut ChainClient<impl ValidatorNodeProvider + Sync, S>,
        contract: PathBuf,
        service: PathBuf,
    ) -> Result<BytecodeId, anyhow::Error>
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        info!("Loading bytecode files...");
        let contract_bytecode = Bytecode::load_from_file(&contract).await.context(format!(
            "failed to load contract bytecode from {:?}",
            &contract
        ))?;
        let service_bytecode = Bytecode::load_from_file(&service).await.context(format!(
            "failed to load service bytecode from {:?}",
            &service
        ))?;

        info!("Publishing bytecode...");
        let (bytecode_id, _cert) = chain_client
            .publish_bytecode(contract_bytecode, service_bytecode)
            .await
            .context("failed to publish bytecode")?;

        info!("{}", "Bytecode published successfully!".green().bold());

        debug!("Synchronizing...");
        chain_client.synchronize_from_validators().await?;
        chain_client.process_inbox().await?;
        Ok(bytecode_id)
    }

    fn generate_key_pair(&mut self) -> KeyPair {
        KeyPair::generate_from(&mut self.prng)
    }
}

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

#[derive(StructOpt)]
#[structopt(
    name = "Linera client tool",
    about = "A Byzantine-fault tolerant sidechain with low-latency finality and high throughput"
)]
struct ClientOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
    #[structopt(long = "wallet")]
    wallet_state_path: Option<PathBuf>,

    /// Storage configuration for the blockchain history.
    #[structopt(long = "storage")]
    storage_config: Option<String>,

    /// Given an integer value N, read the wallet state and the wallet storage config from the
    /// environment variables LINERA_WALLET_{N} and LINERA_STORAGE_{N} instead of
    /// LINERA_WALLET and LINERA_STORAGE.
    #[structopt(long, short = "w")]
    with_wallet: Option<u32>,

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

    /// The WebAssembly runtime to use.
    #[structopt(long)]
    wasm_runtime: Option<WasmRuntime>,

    /// The maximal number of simultaneous queries to the database
    #[structopt(long)]
    max_concurrent_queries: Option<usize>,

    /// The maximal number of simultaneous stream queries to the database
    #[structopt(long, default_value = "10")]
    max_stream_queries: usize,

    /// The maximal number of entries in the storage cache.
    #[structopt(long, default_value = "1000")]
    cache_size: usize,

    /// Subcommand.
    #[structopt(subcommand)]
    command: ClientCommand,

    /// Delay increment for retrying to connect to a validator for notifications.
    #[structopt(long, default_value = "1000000")]
    notification_retry_delay_us: u64,

    /// Number of times to retry connecting to a validator for notifications.
    #[structopt(long, default_value = "10")]
    notification_retries: u32,

    /// Whether to wait until a quorum of validators has confirmed that all sent cross-chain
    /// messages have been delivered.
    #[structopt(long)]
    wait_for_outgoing_messages: bool,
}

impl ClientOptions {
    fn init() -> Result<Self, anyhow::Error> {
        let mut options = ClientOptions::from_args();
        let suffix = match options.with_wallet {
            None => String::new(),
            Some(n) => format!("_{}", n),
        };
        let wallet_env_var = env::var(format!("LINERA_WALLET{suffix}")).ok();
        let storage_env_var = env::var(format!("LINERA_STORAGE{suffix}")).ok();
        if let (None, Some(wallet_path)) = (&options.wallet_state_path, wallet_env_var) {
            options.wallet_state_path = Some(wallet_path.parse()?);
        }
        if let (None, Some(storage_path)) = (&options.storage_config, storage_env_var) {
            options.storage_config = Some(storage_path.parse()?);
        }
        Ok(options)
    }

    async fn run_command_with_storage(self) -> Result<(), Error> {
        let context = ClientContext::from_options(&self)?;
        let genesis_config = context.wallet_state.genesis_config().clone();
        let wasm_runtime = self.wasm_runtime.with_wasm_default();
        let max_concurrent_queries = self.max_concurrent_queries;
        let max_stream_queries = self.max_stream_queries;
        let cache_size = self.cache_size;
        let storage_config = ClientContext::storage_config(&self)?;
        let common_config = CommonStoreConfig {
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        };
        let full_storage_config = storage_config.add_common_config(common_config).await?;
        run_with_storage(
            full_storage_config,
            &genesis_config,
            wasm_runtime,
            Job(context, self.command),
        )
        .await?;
        Ok(())
    }

    async fn initialize_storage(&self) -> Result<(), Error> {
        let context = ClientContext::from_options(self)?;
        let genesis_config = context.wallet_state.genesis_config().clone();
        let max_concurrent_queries = self.max_concurrent_queries;
        let max_stream_queries = self.max_stream_queries;
        let cache_size = self.cache_size;
        let storage_config = ClientContext::storage_config(self)?;
        let common_config = CommonStoreConfig {
            max_concurrent_queries,
            max_stream_queries,
            cache_size,
        };
        let full_storage_config = storage_config.add_common_config(common_config).await?;
        full_initialize_storage(full_storage_config, &genesis_config).await?;
        Ok(())
    }
}

#[derive(StructOpt)]
enum ClientCommand {
    /// Transfer funds
    Transfer {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: Account,

        /// Recipient account
        #[structopt(long = "to")]
        recipient: Account,

        /// Amount to transfer
        amount: Amount,
    },

    /// Open (i.e. activate) a new chain deriving the UID from an existing one.
    OpenChain {
        /// Chain id (must be one of our chains).
        #[structopt(long = "from")]
        chain_id: Option<ChainId>,

        /// Public key of the new owner (otherwise create a key pair and remember it)
        #[structopt(long = "to-public-key")]
        public_key: Option<PublicKey>,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[structopt(long = "initial-balance", default_value = "0")]
        balance: Amount,
    },

    /// Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one.
    OpenMultiOwnerChain {
        /// Chain id (must be one of our chains).
        #[structopt(long = "from")]
        chain_id: Option<ChainId>,

        /// Public keys of the new owners
        #[structopt(long = "to-public-keys")]
        public_keys: Vec<PublicKey>,

        /// Weights for the new owners
        #[structopt(long = "weights")]
        weights: Vec<u64>,

        /// The number of rounds in which every owner can propose blocks, i.e. the first round
        /// number in which only a single designated leader is allowed to propose blocks.
        #[structopt(long = "multi-leader-rounds")]
        multi_leader_rounds: Option<u32>,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[structopt(long = "initial-balance", default_value = "0")]
        balance: Amount,
    },

    /// Close (i.e. deactivate) an existing chain.
    CloseChain {
        /// Chain id (must be one of our chains)
        #[structopt(long = "from")]
        chain_id: ChainId,
    },

    /// Read the balance of the chain from the local state of the client.
    QueryBalance {
        /// Chain id
        chain_id: Option<ChainId>,
    },

    /// Synchronize the local state of the chain (including a conservative estimation of the
    /// available balance) with a quorum validators.
    SyncBalance {
        /// Chain id
        chain_id: Option<ChainId>,
    },

    /// Show the current set of validators for a chain.
    QueryValidators {
        /// Chain id
        chain_id: Option<ChainId>,
    },

    /// Add or modify a validator (admin only)
    SetValidator {
        /// The public key of the validator.
        #[structopt(long)]
        name: ValidatorName,

        /// Network address
        #[structopt(long)]
        address: String,

        /// Voting power
        #[structopt(long, default_value = "1")]
        votes: u64,
    },

    /// Remove a validator (admin only)
    RemoveValidator {
        /// The public key of the validator.
        #[structopt(long)]
        name: ValidatorName,
    },

    /// View or update the resource control policy
    ResourceControlPolicy {
        /// Set the base price for each certificate.
        #[structopt(long)]
        certificate: Option<Amount>,

        /// Set the price per unit of fuel when executing user messages and operations.
        #[structopt(long)]
        fuel: Option<Amount>,

        /// Set the price per byte to read data per operation
        #[structopt(long)]
        storage_num_reads: Option<Amount>,

        /// Set the price per byte to read data per byte
        #[structopt(long)]
        storage_bytes_read: Option<Amount>,

        /// Set the price per byte to write data per byte
        #[structopt(long)]
        storage_bytes_written: Option<Amount>,

        /// Set the maximum quantity of data to read per block
        #[structopt(long)]
        maximum_bytes_read_per_block: Option<u64>,

        /// Set the maximum quantity of data to write per block
        #[structopt(long)]
        maximum_bytes_written_per_block: Option<u64>,

        /// Set the price per byte to store and send outgoing cross-chain messages.
        #[structopt(long)]
        messages: Option<Amount>,
    },

    /// Send one transfer per chain in bulk mode
    #[cfg(feature = "benchmark")]
    Benchmark {
        /// Maximum number of blocks in flight
        #[structopt(long, default_value = "200")]
        max_in_flight: u64,

        /// Use a subset of the chains to generate N transfers
        #[structopt(long)]
        max_proposals: Option<usize>,
    },

    /// Create genesis configuration for a Linera deployment.
    /// Create initial user chains and print information to be used for initialization of validator setup.
    /// This will also create an initial wallet for the owner of the initial "root" chains.
    CreateGenesisConfig {
        /// Sets the file describing the public configurations of all validators
        #[structopt(long = "committee")]
        committee_config_path: PathBuf,

        /// The output config path to be consumed by the server
        #[structopt(long = "genesis")]
        genesis_config_path: PathBuf,

        /// Index of the admin chain in the genesis config
        #[structopt(long, default_value = "0")]
        admin_root: u32,

        /// Known initial balance of the chain
        #[structopt(long, default_value = "0")]
        initial_funding: Amount,

        /// The start timestamp: no blocks can be created before this time.
        #[structopt(long)]
        start_timestamp: Option<DateTime<Utc>>,

        /// Number of additional chains to create
        num: u32,

        /// Set the base price for each certificate.
        #[structopt(long, default_value = "0")]
        certificate_price: Amount,

        /// Set the price per unit of fuel when executing user messages and operations.
        #[structopt(long, default_value = "0")]
        fuel_price: Amount,

        /// Set the price per operation to read data
        #[structopt(long, default_value = "0")]
        storage_num_reads_price: Amount,

        /// Set the price per byte to read data
        #[structopt(long, default_value = "0")]
        storage_bytes_read_price: Amount,

        /// Set the price per byte to write data
        #[structopt(long, default_value = "0")]
        storage_bytes_written_price: Amount,

        /// Set the maximum read data per block
        #[structopt(long)]
        maximum_bytes_read_per_block: Option<u64>,

        /// Set the maximum write data per block
        #[structopt(long)]
        maximum_bytes_written_per_block: Option<u64>,

        /// Set the price per byte to store and send outgoing cross-chain messages.
        #[structopt(long, default_value = "0")]
        messages_price: Amount,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[structopt(long)]
        testing_prng_seed: Option<u64>,

        /// A unique name to identify this network.
        #[structopt(long)]
        network_name: Option<String>,
    },

    /// Watch the network for notifications.
    Watch {
        /// The chain id to watch.
        chain_id: Option<ChainId>,

        /// Show all notifications from all validators.
        #[structopt(long)]
        raw: bool,
    },

    /// Run a GraphQL service to explore and extend the chains of the wallet.
    Service {
        #[structopt(flatten)]
        config: ChainListenerConfig,

        /// The port on which to run the server
        #[structopt(long = "port", default_value = "8080")]
        port: NonZeroU16,
    },

    /// Run a GraphQL service that exposes a faucet where users can claim tokens.
    /// This gives away the chain's tokens, and is mainly intended for testing.
    Faucet {
        /// The chain that gives away its tokens.
        chain_id: Option<ChainId>,

        /// The port on which to run the server
        #[structopt(long = "port", default_value = "8080")]
        port: NonZeroU16,

        /// The number of tokens to send to each new chain.
        #[structopt(long = "amount")]
        amount: Amount,

        /// The end timestamp: The faucet will rate-limit the token supply so it runs out of money
        /// no earlier than this.
        #[structopt(long)]
        limit_rate_until: Option<DateTime<Utc>>,
    },

    /// Publish bytecode.
    PublishBytecode {
        /// Path to the Wasm file for the application "contract" bytecode.
        contract: PathBuf,

        /// Path to the Wasm file for the application "service" bytecode.
        service: PathBuf,

        /// An optional chain ID to publish the bytecode. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,
    },

    /// Create an application.
    CreateApplication {
        /// The bytecode ID of the application to create.
        bytecode_id: BytecodeId,

        /// An optional chain ID to host the application. The default chain of the wallet
        /// is used otherwise.
        creator: Option<ChainId>,

        /// The shared parameters as JSON string.
        #[structopt(long)]
        json_parameters: Option<String>,

        /// Path to a JSON file containing the shared parameters.
        #[structopt(long)]
        json_parameters_path: Option<PathBuf>,

        /// The initialization argument as a JSON string.
        #[structopt(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the initialization argument.
        #[structopt(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[structopt(long)]
        required_application_ids: Option<Vec<UserApplicationId>>,
    },

    /// Create an application, and publish the required bytecode.
    PublishAndCreate {
        /// Path to the Wasm file for the application "contract" bytecode.
        contract: PathBuf,

        /// Path to the Wasm file for the application "service" bytecode.
        service: PathBuf,

        /// An optional chain ID to publish the bytecode. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,

        /// The shared parameters as JSON string.
        #[structopt(long)]
        json_parameters: Option<String>,

        /// Path to a JSON file containing the shared parameters.
        #[structopt(long)]
        json_parameters_path: Option<PathBuf>,

        /// The initialization argument as a JSON string.
        #[structopt(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the initialization argument.
        #[structopt(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[structopt(long)]
        required_application_ids: Option<Vec<UserApplicationId>>,
    },

    /// Request an application from another chain, so it can be used on this one.
    RequestApplication {
        /// The ID of the application to request.
        application_id: UserApplicationId,

        /// The target chain on which the application is already registered.
        /// If not specified, the chain on which the application was created is used.
        #[structopt(long)]
        target_chain_id: Option<ChainId>,

        /// The owned chain on which the application is missing.
        #[structopt(long)]
        requester_chain_id: Option<ChainId>,
    },

    /// Create an unassigned key-pair.
    Keygen,

    /// Link a key owned by the wallet to a chain that was just created for that key.
    Assign {
        /// The public key to assign.
        #[structopt(long)]
        key: PublicKey,

        /// The ID of the message that created the chain. (This uniquely describes the
        /// chain and where it was created.)
        #[structopt(long)]
        message_id: MessageId,
    },

    /// Retry a block we unsuccessfully tried to propose earlier.
    ///
    /// As long as a block is pending most other commands will fail, since it is unsafe to propose
    /// multiple blocks at the same height.
    RetryPendingBlock {
        /// The chain with the pending block. If not specified, the wallet's default chain is used.
        chain_id: Option<ChainId>,
    },

    /// Show the contents of the wallet.
    Wallet(WalletCommand),

    /// Manage Linera projects.
    Project(ProjectCommand),

    /// Manage a local Linera Network.
    Net(NetCommand),
}

#[derive(StructOpt)]
enum NetCommand {
    /// Start a Local Linera Network
    Up {
        /// The number of extra wallets and user chains to initialise. Default is 0.
        #[structopt(long)]
        extra_wallets: Option<usize>,

        /// The number of validators in the local test network. Default is 1.
        #[structopt(long, default_value = "1")]
        validators: usize,

        /// The number of shards per validator in the local test network. Default is 1.
        #[structopt(long, default_value = "1")]
        shards: usize,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[structopt(long)]
        testing_prng_seed: Option<u64>,

        /// The name for the database table to store the chain data in.
        #[structopt(long, default_value = "table_default")]
        table_name: String,

        /// Start the local network on a local Kubernetes deployment.
        #[cfg(feature = "kubernetes")]
        #[structopt(long, short = "k8s")]
        kubernetes: bool,

        /// Directory where we should grab the binaries to be packaged in the Docker image
        /// and run inside the Kubernetes deployment.
        #[cfg(feature = "kubernetes")]
        #[structopt(long)]
        binaries_dir: Option<PathBuf>,

        /// Kind name for the cluster that will be created for the Kubernetes deployment.
        /// Must be a number. If not specified, a random number will be generated.
        #[cfg(feature = "kubernetes")]
        #[structopt(long)]
        cluster_id: Option<u32>,
    },

    /// Print a bash helper script to make `linera net up` easier to use. The script is
    /// meant to be installed in `~/.bash_profile` or sourced when needed.
    Helper,
}

#[derive(StructOpt)]
enum WalletCommand {
    /// Show the contents of the wallet.
    Show { chain_id: Option<ChainId> },

    /// Change the wallet default chain.
    SetDefault { chain_id: ChainId },

    /// Initialize a wallet from the genesis configuration.
    Init {
        /// The path to the genesis configuration for a Linera deployment. Either this or `--faucet`
        /// must be specified.
        #[structopt(long = "genesis")]
        genesis_config_path: Option<PathBuf>,

        /// The address of a faucet. If this is specified, the default chain will be newly created,
        /// and credited with tokens.
        #[structopt(long = "faucet")]
        faucet: Option<String>,

        /// Other chains to follow.
        #[structopt(long)]
        with_other_chains: Vec<ChainId>,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[structopt(long)]
        testing_prng_seed: Option<u64>,
    },
}

#[derive(StructOpt)]
enum ProjectCommand {
    /// Create a new Linera project.
    New {
        /// The project name. A directory of the same name will be created in the current directory.
        name: String,

        /// Use the given clone of the Linera repository instead of remote crates.
        #[structopt(long)]
        linera_root: Option<PathBuf>,
    },

    /// Test a Linera project.
    ///
    /// Equivalent to running `cargo test` with the appropriate test runner.
    Test { path: Option<PathBuf> },

    /// Build and publish a Linera project.
    PublishAndCreate {
        /// The path of the root of the Linera project.
        /// Defaults to current working directory if unspecified.
        path: Option<PathBuf>,

        /// Specify the name of the Linera project.
        /// This is used to locate the generated bytecode. The generated bytecode should
        /// be of the form `<name>_{contract,service}.wasm`.
        ///
        /// Defaults to the package name in Cargo.toml.
        name: Option<String>,

        /// An optional chain ID to publish the bytecode. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,

        /// The shared parameters as JSON string.
        #[structopt(long)]
        json_parameters: Option<String>,

        /// Path to a JSON file containing the shared parameters.
        #[structopt(long)]
        json_parameters_path: Option<PathBuf>,

        /// The initialization argument as a JSON string.
        #[structopt(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the initialization argument.
        #[structopt(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[structopt(long)]
        required_application_ids: Option<Vec<UserApplicationId>>,
    },
}

struct Job(ClientContext, ClientCommand);

fn read_json(string: Option<String>, path: Option<PathBuf>) -> Result<Vec<u8>, anyhow::Error> {
    let value = match (string, path) {
        (Some(_), Some(_)) => bail!("cannot have both a json string and file"),
        (Some(s), None) => serde_json::from_str(&s)?,
        (None, Some(path)) => {
            let s = fs::read_to_string(path)?;
            serde_json::from_str(&s)?
        }
        (None, None) => Value::Null,
    };
    Ok(serde_json::to_vec(&value)?)
}

#[async_trait]
impl Runnable for Job {
    type Output = ();

    async fn run<S>(self, storage: S) -> Result<(), anyhow::Error>
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let Job(mut context, command) = self;
        use ClientCommand::*;
        match command {
            Transfer {
                sender,
                recipient,
                amount,
            } => {
                let mut chain_client = context.make_chain_client(storage, sender.chain_id);
                info!("Starting transfer");
                let time_start = Instant::now();
                let result = chain_client
                    .transfer_to_account(sender.owner, amount, recipient, UserData::default())
                    .await;
                context.update_and_save_wallet(&mut chain_client).await;
                let certificate = result.context("failed to make transfer")?;
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                debug!("{:?}", certificate);
            }

            OpenChain {
                chain_id,
                public_key,
                balance,
            } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                let (new_public_key, key_pair) = match public_key {
                    Some(key) => (key, None),
                    None => {
                        let key_pair = context.generate_key_pair();
                        (key_pair.public(), Some(key_pair))
                    }
                };
                info!("Starting operation to open a new chain");
                let time_start = Instant::now();
                let ownership = ChainOwnership::single(new_public_key);
                let result = chain_client.open_chain(ownership, balance).await;
                context.update_and_save_wallet(&mut chain_client).await;
                let (message_id, certificate) = result.context("failed to open chain")?;
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
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                debug!("{:?}", certificate);
                // Print the new chain ID and message ID on stdout for scripting purposes.
                println!("{}", message_id);
                println!("{}", ChainId::child(message_id));
            }

            OpenMultiOwnerChain {
                chain_id,
                public_keys,
                weights,
                multi_leader_rounds,
                balance,
            } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Starting operation to open a new chain");
                let time_start = Instant::now();
                let owners = if weights.is_empty() {
                    public_keys
                        .into_iter()
                        .zip(iter::repeat(100))
                        .collect::<Vec<_>>()
                } else if weights.len() != public_keys.len() {
                    bail!(
                        "There are {} public keys but {} weights.",
                        public_keys.len(),
                        weights.len()
                    );
                } else {
                    public_keys.into_iter().zip(weights).collect::<Vec<_>>()
                };
                let multi_leader_rounds = multi_leader_rounds.unwrap_or(u32::MAX);
                let ownership = ChainOwnership::multiple(owners, multi_leader_rounds);
                let result = chain_client.open_chain(ownership, balance).await;
                context.update_and_save_wallet(&mut chain_client).await;
                let (message_id, certificate) = result.context("failed to open chain")?;
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
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                debug!("{:?}", certificate);
                // Print the new chain ID and message ID on stdout for scripting purposes.
                println!("{}", message_id);
                println!("{}", ChainId::child(message_id));
            }

            CloseChain { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Starting operation to close the chain");
                let time_start = Instant::now();
                let result = chain_client.close_chain().await;
                context.update_and_save_wallet(&mut chain_client).await;
                let certificate = result.context("failed to close chain")?;
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                debug!("{:?}", certificate);
            }

            QueryBalance { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Starting query for the local balance");
                let time_start = Instant::now();
                let result = chain_client.local_balance().await;
                context.update_and_save_wallet(&mut chain_client).await;
                let balance = result.context("Use sync_balance instead")?;
                let time_total = time_start.elapsed().as_micros();
                info!("Local balance obtained after {} us", time_total);
                println!("{}", balance);
            }

            SyncBalance { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Synchronize chain information");
                let time_start = Instant::now();
                let result = chain_client.synchronize_from_validators().await;
                context.update_and_save_wallet(&mut chain_client).await;
                let balance = result.context("Failed to synchronize from validators")?;
                let time_total = time_start.elapsed().as_micros();
                info!("Chain balance synchronized after {} us", time_total);
                println!("{}", balance);
            }

            QueryValidators { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Starting operation to query validators");
                let time_start = Instant::now();
                let result = chain_client.local_committee().await;
                context.update_and_save_wallet(&mut chain_client).await;
                let committee = result.context("Failed to get local committee")?;
                let time_total = time_start.elapsed().as_micros();
                info!("Validators obtained after {} us", time_total);
                info!("{:?}", committee.validators());
            }

            command @ (SetValidator { .. }
            | RemoveValidator { .. }
            | ResourceControlPolicy { .. }) => {
                info!("Starting operations to change validator set");
                let time_start = Instant::now();

                // Make sure genesis chains are subscribed to the admin chain.
                let certificates = context.ensure_admin_subscription(&storage).await;
                let mut chain_client = context
                    .make_chain_client(storage.clone(), context.wallet_state.genesis_admin_chain());
                for cert in certificates {
                    chain_client.receive_certificate(cert).await.unwrap();
                }
                let n = chain_client
                    .process_inbox()
                    .await
                    .unwrap()
                    .into_iter()
                    .filter_map(|c| c.value().executed_block().map(|e| e.messages.len()))
                    .sum::<usize>();
                info!("Subscribed {} chains to new committees", n);

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
                            return Ok(());
                        }
                    }
                    ResourceControlPolicy {
                        certificate,
                        fuel,
                        storage_num_reads,
                        storage_bytes_read,
                        storage_bytes_written,
                        maximum_bytes_read_per_block,
                        maximum_bytes_written_per_block,
                        messages,
                    } => {
                        if let Some(certificate) = certificate {
                            policy.certificate = certificate;
                        }
                        if let Some(fuel) = fuel {
                            policy.fuel = fuel;
                        }
                        if let Some(storage_num_reads) = storage_num_reads {
                            policy.storage_num_reads = storage_num_reads;
                        }
                        if let Some(storage_bytes_read) = storage_bytes_read {
                            policy.storage_bytes_read = storage_bytes_read;
                        }
                        if let Some(storage_bytes_written) = storage_bytes_written {
                            policy.storage_bytes_written = storage_bytes_written;
                        }
                        if let Some(maximum_bytes_read_per_block) = maximum_bytes_read_per_block {
                            policy.maximum_bytes_read_per_block = maximum_bytes_read_per_block;
                        }
                        if let Some(maximum_bytes_written_per_block) =
                            maximum_bytes_written_per_block
                        {
                            policy.maximum_bytes_written_per_block =
                                maximum_bytes_written_per_block;
                        }
                        if let Some(messages) = messages {
                            policy.messages = messages;
                        }
                        info!(
                            "ResourceControlPolicy:\n\
                            {:.2} base cost per block\n\
                            {:.2} cost per byte of operations and incoming messages\n\
                            {:.2} cost per byte operation\n\
                            {:.2} cost per bytes read\n\
                            {:.2} cost per bytes written\n\
                            {:.2} per byte of outgoing messages\n\
                            {:.2} maximum number bytes read per block\n\
                            {:.2} maximum number bytes written per block",
                            policy.certificate,
                            policy.fuel,
                            policy.storage_num_reads,
                            policy.storage_bytes_read,
                            policy.storage_bytes_written,
                            policy.messages,
                            policy.maximum_bytes_read_per_block,
                            policy.maximum_bytes_written_per_block
                        );
                        if certificate.is_none()
                            && fuel.is_none()
                            && storage_num_reads.is_none()
                            && storage_bytes_read.is_none()
                            && storage_bytes_written.is_none()
                            && maximum_bytes_read_per_block.is_none()
                            && maximum_bytes_written_per_block.is_none()
                            && messages.is_none()
                        {
                            return Ok(());
                        }
                    }
                    _ => unreachable!(),
                }
                committee = Committee::new(validators, policy);
                let result = chain_client.stage_new_committee(committee).await;
                context.update_and_save_wallet(&mut chain_client).await;
                let certificate = result.context("failed to stage committee")?;
                info!("Staging committee:\n{:?}", certificate);
                context.push_to_all_chains(&storage, &certificate).await;

                // Remove the old committee.
                let result = chain_client.finalize_committee().await;
                context.update_and_save_wallet(&mut chain_client).await;
                let certificate = result.context("failed to finalize committee")?;
                info!("Finalizing committee:\n{:?}", certificate);
                context.push_to_all_chains(&storage, &certificate).await;
                context.save_wallet();

                let time_total = time_start.elapsed().as_micros();
                info!("Operations confirmed after {} us", time_total);
            }

            #[cfg(feature = "benchmark")]
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
                info!("Starting benchmark phase 1 (block proposals)");
                let proposals = context.make_benchmark_block_proposals(max_proposals);
                let num_proposal = proposals.len();
                let mut values = HashMap::new();

                for rpc_msg in &proposals {
                    if let RpcMessage::BlockProposal(proposal) = rpc_msg {
                        let (executed_block, _) =
                            WorkerState::new("staging".to_string(), None, storage.clone())
                                .stage_block_execution(proposal.content.block.clone())
                                .await?;
                        let value =
                            HashedValue::from(CertificateValue::ConfirmedBlock { executed_block });
                        values.insert(value.hash(), value);
                    }
                }

                let responses = context
                    .mass_broadcast("block proposals", max_in_flight, proposals)
                    .await;
                let votes = responses
                    .into_iter()
                    .filter_map(|message| {
                        deserialize_response(message).and_then(|response| {
                            response.info.manager.pending.and_then(|vote| {
                                let value = values.get(&vote.value.value_hash)?.clone();
                                vote.clone().with_value(value)
                            })
                        })
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
                            blobs: vec![],
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
                context
                    .update_wallet_from_certificates(storage, certificates)
                    .await;
                context.save_wallet();
            }

            Watch { chain_id, raw } => {
                let chain_client = context.make_chain_client(storage, chain_id);
                let chain_id = chain_client.chain_id();
                info!("Watching for notifications for chain {:?}", chain_id);
                let mut tracker = NotificationTracker::default();
                let mut notification_stream =
                    ChainClient::listen(Arc::new(Mutex::new(chain_client))).await?;
                while let Some(notification) = notification_stream.next().await {
                    if raw || tracker.insert(notification.clone()) {
                        println!("{:?}", notification);
                    }
                }
                info!("Notification stream ended.");
                // Not saving the wallet because `listen()` does not create blocks.
            }

            Service { config, port } => {
                let default_chain = context.wallet_state.default_chain();
                let service = NodeService::new(config, port, default_chain, storage, context);
                service.run().await?;
            }

            Faucet {
                chain_id,
                port,
                amount,
                limit_rate_until,
            } => {
                let chain_client = context.make_chain_client(storage, chain_id);
                let end_timestamp = limit_rate_until
                    .map(|et| {
                        let micros = u64::try_from(et.timestamp_micros())
                            .expect("End timestamp before 1970");
                        Timestamp::from(micros)
                    })
                    .unwrap_or_else(Timestamp::now);
                let genesis_config = Arc::new(context.wallet_state.genesis_config().clone());
                let faucet =
                    FaucetService::new(port, chain_client, amount, end_timestamp, genesis_config)
                        .await?;
                faucet.run().await?;
            }

            PublishBytecode {
                contract,
                service,
                publisher,
            } => {
                let start_time = Instant::now();
                let mut chain_client = context.make_chain_client(storage, publisher);
                let result = context
                    .publish_bytecode(&mut chain_client, contract, service)
                    .await;
                context.update_and_save_wallet(&mut chain_client).await;
                let bytecode_id = result.context("failed to publish bytecode")?;
                println!("{}", bytecode_id);
                info!("Time elapsed: {}s", start_time.elapsed().as_secs());
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
                let mut chain_client = context.make_chain_client(storage, creator);

                info!("Processing arguments...");
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                info!("Synchronizing...");
                chain_client.synchronize_from_validators().await?;
                chain_client.process_inbox().await?;

                info!("Creating application...");
                let result = chain_client
                    .create_application_untyped(
                        bytecode_id,
                        parameters,
                        argument,
                        required_application_ids.unwrap_or_default(),
                    )
                    .await;
                context.update_and_save_wallet(&mut chain_client).await;
                let (application_id, _) = result.context("failed to create application")?;
                info!("{}", "Application created successfully!".green().bold());
                println!("{}", application_id);
                info!("Time elapsed: {}s", start_time.elapsed().as_secs());
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
                let mut chain_client = context.make_chain_client(storage, publisher);

                info!("Processing arguments...");
                let parameters = read_json(json_parameters, json_parameters_path)?;
                let argument = read_json(json_argument, json_argument_path)?;

                let result = context
                    .publish_bytecode(&mut chain_client, contract, service)
                    .await;
                context.update_and_save_wallet(&mut chain_client).await;
                let bytecode_id = result.context("failed to publish bytecode")?;

                info!("Creating application...");
                let result = chain_client
                    .create_application_untyped(
                        bytecode_id,
                        parameters,
                        argument,
                        required_application_ids.unwrap_or_default(),
                    )
                    .await;
                context.update_and_save_wallet(&mut chain_client).await;
                let (application_id, _) = result.context("failed to create application")?;

                info!("{}", "Application published successfully!".green().bold());
                println!("{}", application_id);
                info!("Time elapsed: {}s", start_time.elapsed().as_secs());
            }

            RequestApplication {
                application_id,
                target_chain_id,
                requester_chain_id,
            } => {
                let mut chain_client = context.make_chain_client(storage, requester_chain_id);
                info!("Starting request");
                let result = chain_client
                    .request_application(application_id, target_chain_id)
                    .await;
                context.update_and_save_wallet(&mut chain_client).await;
                let certificate = result.context("failed to create application")?;
                debug!("{:?}", certificate);
            }

            Assign { key, message_id } => {
                let chain_id = ChainId::child(message_id);
                Self::assign_new_chain_to_key(chain_id, message_id, storage, key, &mut context)
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
                    let mut chain_client = context.make_chain_client(storage, publisher);

                    info!("Processing arguments...");
                    let parameters = read_json(json_parameters, json_parameters_path)?;
                    let argument = read_json(json_argument, json_argument_path)?;
                    let project_path = path.unwrap_or_else(|| env::current_dir().unwrap());

                    let project = project::Project::from_existing_project(project_path)?;
                    let (contract_path, service_path) = project.build(name)?;

                    let result = context
                        .publish_bytecode(&mut chain_client, contract_path, service_path)
                        .await;
                    context.update_and_save_wallet(&mut chain_client).await;
                    let bytecode_id = result.context("failed to publish bytecode")?;

                    info!("Creating application...");
                    let result = chain_client
                        .create_application_untyped(
                            bytecode_id,
                            parameters,
                            argument,
                            required_application_ids.unwrap_or_default(),
                        )
                        .await;
                    context.update_and_save_wallet(&mut chain_client).await;
                    let (application_id, _) = result.context("failed to create application")?;

                    info!("{}", "Application published successfully!".green().bold());
                    println!("{}", application_id);
                    info!("Time elapsed: {}s", start_time.elapsed().as_secs());
                }
                _ => unreachable!("other project commands do not require storage"),
            },

            RetryPendingBlock { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                if let Some(certificate) = chain_client.retry_pending_block().await? {
                    info!("Pending block committed successfully.");
                    println!("{}", certificate.hash());
                } else {
                    info!("No block is currently pending.");
                }
                context.update_and_save_wallet(&mut chain_client).await;
            }

            Wallet(WalletCommand::Init {
                faucet: Some(faucet_url),
                with_other_chains,
                ..
            }) => {
                info!("Requesting a new chain from the faucet.");
                let key_pair = context.generate_key_pair();
                let public_key = key_pair.public();
                context.wallet_state.add_unassigned_key_pair(key_pair);
                let outcome = cli_wrappers::Faucet::claim_url(&public_key, &faucet_url).await?;
                println!("{}", outcome.chain_id);
                println!("{}", outcome.message_id);
                println!("{}", outcome.certificate_hash);
                Self::assign_new_chain_to_key(
                    outcome.chain_id,
                    outcome.message_id,
                    storage.clone(),
                    public_key,
                    &mut context,
                )
                .await?;
                let admin_id = context.wallet_state.genesis_admin_chain();
                let chains = with_other_chains
                    .into_iter()
                    .chain([admin_id, outcome.chain_id]);
                Self::print_peg_certificate_hash(storage, chains, &context).await?;
                context.wallet_state.set_default_chain(outcome.chain_id)?;
                context.save_wallet();
            }

            CreateGenesisConfig { .. } | Keygen | Net(_) | Wallet(_) => unreachable!(),
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
        context: &mut ClientContext,
    ) -> anyhow::Result<()>
    where
        S: Store + Clone + Send + Sync + 'static,
        ViewError: From<S::ContextError>,
    {
        let state = WorkerState::new("Local node".to_string(), None, storage)
            .with_allow_inactive_chains(true)
            .with_allow_messages_from_deprecated_epochs(true);
        let mut node_client = LocalNodeClient::new(state, Notifier::default());

        // Take the latest committee we know of.
        let admin_chain_id = context.wallet_state.genesis_admin_chain();
        let query = ChainInfoQuery::new(admin_chain_id).with_committees();
        let info = node_client.handle_chain_info_query(query).await?;
        let committee = info
            .latest_committee()
            .context("Invalid chain info response; missing latest committee")?;
        let nodes = context.make_node_provider().make_nodes(committee)?;

        // Download the parent chain.
        let target_height = message_id.height.try_add_one()?;
        node_client
            .download_certificates(nodes, message_id.chain_id, target_height)
            .await
            .context("failed to download parent chain")?;

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
        let Some(Message::System(SystemMessage::OpenChain { ownership, .. })) = executed_block
            .message_by_id(&message_id)
            .map(|msg| &msg.message)
        else {
            bail!(
                "The message with the ID returned by the faucet is not OpenChain. \
                Please make sure you are connecting to a genuine faucet."
            );
        };
        anyhow::ensure!(
            ownership.verify_owner(&Owner::from(public_key)) == Some(public_key),
            "The chain with the ID returned by the faucet is not owned by you. \
            Please make sure you are connecting to a genuine faucet."
        );
        context
            .wallet_state
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
        context: &ClientContext,
    ) -> anyhow::Result<()>
    where
        S: Store + Clone + Send + Sync + 'static,
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
        let config_hash = CryptoHash::new(context.wallet_state.genesis_config());
        let maybe_epoch = peg_chain.execution_state.system.epoch.get();
        let epoch = maybe_epoch.context("missing epoch in peg chain")?.0;
        warn!(
            "Initialized wallet based on data provided by untrusted nodes. \
            Please verify that the following is correct by comparing with a trusted user \
            who is already connected to the network, and that {epoch} is the current epoch:\n\
            genesis config hash: {config_hash}"
        );
        if let Some(peg_hash) = peg_chain.tip_state.get().block_hash {
            warn!(
                "In addition, verify that the following certificate exists in the real network:\n\
                certificate hash: {peg_hash}\n\
                chain ID:         {peg_chain_id}"
            );
        }
        Ok(())
    }
}

async fn net_up(
    extra_wallets: &Option<usize>,
    mut net: impl LineraNet,
    client1: ClientWrapper,
) -> Result<(), anyhow::Error> {
    let default_chain = client1
        .default_chain()
        .expect("Initialized clients should always have a default chain");

    // Make time to (hopefully) display the message after the tracing logs.
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Create the wallet for the initial "root" chains.
    info!("Local test network successfully started.");
    let suffix = if let Some(extra_wallets) = *extra_wallets {
        eprintln!(
            "To use the initial wallet and the extra wallets of this test \
            network, you may set the environment variables LINERA_WALLET_$N \
            and LINERA_STORAGE_$N (N = 0..={extra_wallets}) as printed on \
            the standard output, then use the option `--with-wallet $N` (or \
            `-w $N` for short) to select a wallet in the linera tool.\n"
        );
        "_0"
    } else {
        eprintln!(
            "To use the initial wallet of this test network, you may set \
            the environment variables LINERA_WALLET and LINERA_STORAGE as follows.\n"
        );
        ""
    };
    println!(
        "{}",
        format!(
            "export LINERA_WALLET{suffix}=\"{}\"",
            client1.wallet_path().display()
        )
        .bold()
    );
    println!(
        "{}",
        format!(
            "export LINERA_STORAGE{suffix}=\"{}\"\n",
            client1.storage_path()
        )
        .bold()
    );

    // Create the extra wallets.
    if let Some(extra_wallets) = *extra_wallets {
        for wallet in 1..=extra_wallets {
            let extra_wallet = net.make_client();
            extra_wallet.wallet_init(&[], None).await?;
            let unassigned_key = extra_wallet.keygen().await?;
            let new_chain_msg_id = client1
                .open_chain(default_chain, Some(unassigned_key))
                .await?
                .0;
            extra_wallet
                .assign(unassigned_key, new_chain_msg_id)
                .await?;
            println!(
                "{}",
                format!(
                    "export LINERA_WALLET_{wallet}=\"{}\"",
                    extra_wallet.wallet_path().display(),
                )
                .bold()
            );
            println!(
                "{}",
                format!(
                    "export LINERA_STORAGE_{wallet}=\"{}\"\n",
                    extra_wallet.storage_path(),
                )
                .bold()
            );
        }
    }

    eprintln!(
        "\nREADY!\nPress ^C to terminate the local test network and clean the temporary directory."
    );
    let mut sigint = unix::signal(unix::SignalKind::interrupt())?;
    let mut sigterm = unix::signal(unix::SignalKind::terminate())?;
    let mut sigpipe = unix::signal(unix::SignalKind::pipe())?;
    let mut sighup = unix::signal(unix::SignalKind::hangup())?;
    tokio::select! {
        _ = sigint.recv() => (),
        _ = sigterm.recv() => (),
        _ = sigpipe.recv() => (),
        _ = sighup.recv() => (),
    }
    eprintln!("\nTerminating the local test network...");
    net.terminate().await?;
    eprintln!("\nDone.");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let env_filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .from_env_lossy();
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .init();
    let options = ClientOptions::init()?;

    match &options.command {
        ClientCommand::CreateGenesisConfig {
            committee_config_path,
            genesis_config_path,
            admin_root,
            initial_funding,
            start_timestamp,
            num,
            certificate_price,
            fuel_price,
            storage_num_reads_price,
            storage_bytes_read_price,
            storage_bytes_written_price,
            maximum_bytes_read_per_block,
            maximum_bytes_written_per_block,
            messages_price,
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
                certificate: *certificate_price,
                fuel: *fuel_price,
                storage_num_reads: *storage_num_reads_price,
                storage_bytes_read: *storage_bytes_read_price,
                storage_bytes_written: *storage_bytes_written_price,
                maximum_bytes_read_per_block,
                maximum_bytes_written_per_block,
                messages: *messages_price,
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
            for i in 0..*num {
                let description = ChainDescription::Root(i);
                // Create keys.
                let chain = UserChain::make_initial(&mut rng, description, timestamp);
                // Public "genesis" state.
                let key = chain.key_pair.as_ref().unwrap().public();
                genesis_config.chains.push((key, *initial_funding));
                // Private keys.
                chains.push(chain);
            }
            let new_prng_seed = if testing_prng_seed.is_some() {
                Some(rng.gen())
            } else {
                None
            };
            let mut context =
                ClientContext::create(&options, genesis_config.clone(), new_prng_seed, chains)?;
            genesis_config.write(genesis_config_path)?;
            context.save_wallet();
            options.initialize_storage().await?;
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
            let mut context = ClientContext::from_options(&options)?;
            let key_pair = context.generate_key_pair();
            let public = key_pair.public();
            context.wallet_state.add_unassigned_key_pair(key_pair);
            context.save_wallet();
            println!("{}", public);
            Ok(())
        }

        ClientCommand::Net(net_command) => match net_command {
            NetCommand::Up {
                extra_wallets,
                validators,
                shards,
                testing_prng_seed,
                table_name,
                #[cfg(feature = "kubernetes")]
                kubernetes,
                #[cfg(feature = "kubernetes")]
                binaries_dir,
                #[cfg(feature = "kubernetes")]
                cluster_id,
            } => {
                if *validators < 1 {
                    panic!("The local test network must have at least one validator.");
                }
                if *shards < 1 {
                    panic!("The local test network must have at least one shard per validator.");
                }

                #[cfg(feature = "kubernetes")]
                if *kubernetes {
                    let config = LocalKubernetesNetConfig {
                        network: Network::Grpc,
                        testing_prng_seed: *testing_prng_seed,
                        binaries_dir: binaries_dir.clone(),
                        cluster_id: *cluster_id,
                    };
                    let (net, client1) = config.instantiate().await?;
                    Ok(net_up(extra_wallets, net, client1).await?)
                } else {
                    let config = LocalNetConfig {
                        network: Network::Grpc,
                        database: Database::RocksDb,
                        testing_prng_seed: *testing_prng_seed,
                        table_name: table_name.to_string(),
                        num_initial_validators: *validators,
                        num_shards: *shards,
                    };
                    let (net, client1) = config.instantiate().await?;
                    Ok(net_up(extra_wallets, net, client1).await?)
                }
                #[cfg(not(feature = "kubernetes"))]
                {
                    let config = LocalNetConfig {
                        network: Network::Grpc,
                        database: Database::RocksDb,
                        testing_prng_seed: *testing_prng_seed,
                        table_name: table_name.to_string(),
                        num_initial_validators: *validators,
                        num_shards: *shards,
                    };
                    let (net, client1) = config.instantiate().await?;
                    Ok(net_up(extra_wallets, net, client1).await?)
                }
            }

            NetCommand::Helper => {
                info!("You may append the following script to your `~/.bash_profile` or `source` it when needed.");
                info!(
                    "This will install a function `linera_spawn_and_read_wallet_variables` to facilitate \
                       testing with a local Linera network"
                );
                println!("{}", include_str!("../template/linera_net_helper.sh"));
                Ok(())
            }
        },

        ClientCommand::Wallet(wallet_command) => match wallet_command {
            WalletCommand::Show { chain_id } => {
                let context = ClientContext::from_options(&options)?;
                context.wallet_state.pretty_print(*chain_id);
                Ok(())
            }
            WalletCommand::SetDefault { chain_id } => {
                let mut context = ClientContext::from_options(&options)?;
                context.wallet_state.set_default_chain(*chain_id)?;
                context.save_wallet();
                Ok(())
            }

            WalletCommand::Init {
                genesis_config_path,
                faucet,
                with_other_chains,
                testing_prng_seed,
            } => {
                let genesis_config = match (genesis_config_path, faucet) {
                    (Some(genesis_config_path), None) => GenesisConfig::read(genesis_config_path)?,
                    (None, Some(url)) => cli_wrappers::Faucet::request_genesis_config(url).await?,
                    (_, _) => bail!("Either --faucet or --genesis must be specified, but not both"),
                };
                let timestamp = genesis_config.timestamp;
                let chains = with_other_chains
                    .iter()
                    .filter_map(|chain_id| {
                        let i = (0..(genesis_config.chains.len() as u32))
                            .find(|i| ChainId::root(*i) == *chain_id)?;
                        let description = ChainDescription::Root(i);
                        Some(UserChain::make_other(description, timestamp))
                    })
                    .collect();
                let mut context =
                    ClientContext::create(&options, genesis_config, *testing_prng_seed, chains)?;
                context.save_wallet();
                options.initialize_storage().await?;
                if faucet.is_some() {
                    options.run_command_with_storage().await?;
                }
                Ok(())
            }
        },

        _ => options.run_command_with_storage().await,
    }
}
