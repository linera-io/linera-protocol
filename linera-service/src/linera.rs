// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::{anyhow, bail, Context, Error};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use colored::Colorize;
use futures::{lock::Mutex, StreamExt};
use linera_base::{
    crypto::{KeyPair, PublicKey},
    data_types::{Amount, BlockHeight, Timestamp},
    identifiers::{BytecodeId, ChainDescription, ChainId, MessageId},
};
use linera_chain::data_types::{Certificate, CertificateValue, ExecutedBlock};
use linera_core::{
    client::{ChainClient, ValidatorNodeProvider},
    data_types::ChainInfoQuery,
    node::{LocalNodeClient, ValidatorNode},
    tracker::NotificationTracker,
    worker::WorkerState,
};
use linera_execution::{
    committee::{Committee, ValidatorName, ValidatorState},
    pricing::Pricing,
    system::{Account, UserData},
    Bytecode, UserApplicationId, WasmRuntime, WithWasmDefault,
};
use linera_rpc::node_provider::{NodeOptions, NodeProvider};
use linera_service::{
    chain_listener::ChainListenerConfig,
    config::{CommitteeConfig, Export, GenesisConfig, Import, UserChain, WalletState},
    node_service::NodeService,
    project::{self, Project},
    storage::{Runnable, StorageConfig},
};
use linera_storage::Store;
use linera_views::views::ViewError;
use serde_json::Value;
use std::{
    env, fs,
    io::Read,
    num::NonZeroU16,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use structopt::StructOpt;
use tracing::{debug, info, warn};

use linera_service::client::{LocalNet, Network};
#[cfg(feature = "benchmark")]
use {
    linera_base::data_types::RoundNumber,
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
    std::collections::{HashMap, HashSet},
    tracing::{error, trace},
};

struct ClientContext {
    wallet_state: WalletState,
    max_pending_messages: usize,
    send_timeout: Duration,
    recv_timeout: Duration,
    cross_chain_delay: Duration,
    cross_chain_retries: usize,
    notification_retry_delay: Duration,
    notification_retries: u32,
    wait_for_outgoing_messages: bool,
}

impl ClientContext {
    fn create(
        options: &ClientOptions,
        genesis_config: GenesisConfig,
        chains: Vec<UserChain>,
    ) -> Result<Self, anyhow::Error> {
        let wallet_state_path = match &options.wallet_state_path {
            Some(path) => path.clone(),
            None => Self::create_default_wallet_path()?,
        };
        if wallet_state_path.exists() {
            bail!(
                "Wallet already exists at {}. Aborting...",
                wallet_state_path.display()
            )
        }
        let mut wallet_state = WalletState::create(&wallet_state_path, genesis_config)
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
        let wallet_state = WalletState::from_file(&wallet_state_path).with_context(|| {
            format!(
                "Unable to read wallet at {:?}:",
                &wallet_state_path.canonicalize().with_context(|| format!(
                    "Unable to canonicalize wallet {:?}",
                    &wallet_state_path
                ))
            )
        })?;
        Ok(Self::configure(options, wallet_state))
    }

    fn configure(options: &ClientOptions, wallet_state: WalletState) -> Self {
        let send_timeout = Duration::from_micros(options.send_timeout_us);
        let recv_timeout = Duration::from_micros(options.recv_timeout_us);
        let cross_chain_delay = Duration::from_micros(options.cross_chain_delay_ms);
        let notification_retry_delay = Duration::from_micros(options.notification_retry_delay_us);
        let notification_retries = options.notification_retries;
        let wait_for_outgoing_messages = options.wait_for_outgoing_messages;

        ClientContext {
            wallet_state,
            max_pending_messages: options.max_pending_messages,
            send_timeout,
            recv_timeout,
            cross_chain_delay,
            cross_chain_retries: options.cross_chain_retries,
            notification_retry_delay,
            notification_retries,
            wait_for_outgoing_messages,
        }
    }

    fn create_default_config_path() -> Result<PathBuf, anyhow::Error> {
        let mut config_dir = dirs::config_dir().ok_or_else(|| {
            anyhow!("Default configuration directory not supported. Please specify a path.")
        })?;
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
            None => Ok(StorageConfig::Rocksdb {
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
                NetworkProtocol::Grpc => Box::new(
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
    ) -> ChainClient<impl ValidatorNodeProvider, S> {
        let chain_id = chain_id.into().unwrap_or_else(|| {
            self.wallet_state
                .default_chain()
                .expect("No chain specified in wallet with no default chain")
        });
        let chain = self
            .wallet_state
            .get(chain_id)
            .unwrap_or_else(|| panic!("Unknown chain: {}", chain_id));
        ChainClient::new(
            chain_id,
            chain
                .key_pair
                .as_ref()
                .map(|kp| kp.copy())
                .into_iter()
                .collect(),
            self.make_node_provider(),
            storage,
            self.wallet_state.genesis_admin_chain(),
            self.max_pending_messages,
            chain.block_hash,
            chain.timestamp,
            chain.next_block_height,
            self.cross_chain_delay,
            self.cross_chain_retries,
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
                epoch: Epoch::from(0),
                chain_id: chain.chain_id,
                incoming_messages: Vec::new(),
                operations: vec![Operation::System(SystemOperation::Transfer {
                    owner: None,
                    recipient: Recipient::Account(Account::chain(next_recipient)),
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
                    round: RoundNumber::default(),
                },
                key_pair,
                vec![],
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
            let chain_id = vote.value.inner().chain_id();
            if done_senders.contains(&chain_id) {
                continue;
            }
            trace!(
                "Processing vote on {:?}'s block by {:?}",
                chain_id,
                vote.validator,
            );
            let aggregator = aggregators.entry(chain_id).or_insert_with(|| {
                SignatureAggregator::new(vote.value, RoundNumber(0), &committee)
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

    /// Remembers the new private key (if any) in the wallet.
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
        let mut node = LocalNodeClient::new(worker);
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
    name = "Linera Client",
    about = "A Byzantine-fault tolerant sidechain with low-latency finality and high throughput"
)]
struct ClientOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
    #[structopt(long = "wallet")]
    wallet_state_path: Option<PathBuf>,

    /// Storage configuration for the blockchain history.
    #[structopt(long = "storage")]
    storage_config: Option<String>,

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
        let mut client_options = ClientOptions::from_args();
        let wallet_env_var = env::var("LINERA_WALLET").ok();
        let storage_env_var = env::var("LINERA_STORAGE").ok();
        if let (None, Some(wallet_path)) = (&client_options.wallet_state_path, wallet_env_var) {
            client_options.wallet_state_path = Some(wallet_path.parse()?);
        }
        if let (None, Some(storage_path)) = (&client_options.storage_config, storage_env_var) {
            client_options.storage_config = Some(storage_path.parse()?);
        }
        Ok(client_options)
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
        /// Sending chain id (must be one of our chains).
        #[structopt(long = "from")]
        sender: Option<ChainId>,

        /// Public key of the new owner (otherwise create a key pair and remember it)
        #[structopt(long = "to-public-key")]
        public_key: Option<PublicKey>,
    },

    /// Close (i.e. deactivate) an existing chain.
    // TODO: Consider `spend-and-transfer` instead for real-life use cases.
    CloseChain {
        /// Sending chain id (must be one of our chains)
        #[structopt(long = "from")]
        sender: ChainId,
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

    /// View or update the pricing.
    Pricing {
        /// Set the base price for each certificate.
        #[structopt(long)]
        certificate: Option<Amount>,

        /// Set the price per unit of fuel when executing user messages and operations.
        #[structopt(long)]
        fuel: Option<Amount>,

        /// Set the price per byte to store a block's operations, incoming and outgoing messages.
        #[structopt(long)]
        storage: Option<Amount>,

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

        /// Set the price per byte to store a block's operations, incoming and outgoing messages.
        #[structopt(long, default_value = "0")]
        storage_price: Amount,

        /// Set the price per byte to store and send outgoing cross-chain messages.
        #[structopt(long, default_value = "0")]
        messages_price: Amount,
    },

    /// Watch the network for notifications.
    Watch {
        /// The chain id to watch.
        chain_id: Option<ChainId>,

        /// Show all notifications from all validators.
        #[structopt(long)]
        raw: bool,
    },

    /// Run a GraphQL service on the local node of a given chain.
    Service {
        /// Chain id
        chain_id: Option<ChainId>,

        #[structopt(flatten)]
        config: ChainListenerConfig,

        /// The port on which to run the server
        #[structopt(long = "port", default_value = "8080")]
        port: NonZeroU16,
    },

    /// Publish bytecode.
    PublishBytecode {
        /// Path to the WASM file for the application "contract" bytecode.
        contract: PathBuf,

        /// Path to the WASM file for the application "service" bytecode.
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
        /// Path to the WASM file for the application "contract" bytecode.
        contract: PathBuf,

        /// Path to the WASM file for the application "service" bytecode.
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

    /// Show the contents of the wallet.
    Wallet(WalletCommand),

    /// Manage Linera projects.
    Project(ProjectCommand),

    Net(NetCommand),
}

#[derive(StructOpt)]
enum NetCommand {
    Up,
}
#[derive(StructOpt)]
enum WalletCommand {
    /// Show the contents of the wallet.
    Show { chain_id: Option<ChainId> },

    /// Change the wallet default chain.
    SetDefault { chain_id: ChainId },

    /// Initialize a wallet from the genesis configuration.
    Init {
        /// The path to the genesis configuration for a Linera deployment.
        #[structopt(long = "genesis")]
        genesis_config_path: PathBuf,

        // Other chains to follow.
        #[structopt(long)]
        with_other_chains: Vec<ChainId>,
    },
}

#[derive(StructOpt)]
enum ProjectCommand {
    /// Create a new Linera project.
    New { path: PathBuf },
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
                let mut chain_client = context.make_chain_client(storage, sender.chain_id);
                info!("Starting transfer");
                let time_start = Instant::now();
                let certificate = chain_client
                    .transfer_to_account(sender.owner, amount, recipient, UserData::default())
                    .await
                    .unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                info!("{:?}", certificate);
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            OpenChain { sender, public_key } => {
                let mut chain_client = context.make_chain_client(storage, sender);
                let (new_public_key, key_pair) = match public_key {
                    Some(key) => (key, None),
                    None => {
                        let key_pair = KeyPair::generate();
                        (key_pair.public(), Some(key_pair))
                    }
                };
                info!("Starting operation to open a new chain");
                let time_start = Instant::now();
                let (message_id, certificate) =
                    chain_client.open_chain(new_public_key).await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                info!("{:#?}", certificate);
                context.update_wallet_from_client(&mut chain_client).await;
                let id = ChainId::child(message_id);
                let timestamp = match certificate.value() {
                    CertificateValue::ConfirmedBlock {
                        executed_block: ExecutedBlock { block, .. },
                        ..
                    } => block.timestamp,
                    _ => panic!("Unexpected certificate."),
                };
                context.update_wallet_for_new_chain(id, key_pair, timestamp);
                // Print the new chain ID and message ID on stdout for scripting purposes.
                println!("{}", message_id);
                println!("{}", ChainId::child(message_id));
                context.save_wallet();
            }

            CloseChain { sender } => {
                let mut chain_client = context.make_chain_client(storage, sender);
                info!("Starting operation to close the chain");
                let time_start = Instant::now();
                let certificate = chain_client.close_chain().await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Operation confirmed after {} us", time_total);
                info!("{:?}", certificate);
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            QueryBalance { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Starting query for the local balance");
                let time_start = Instant::now();
                let balance = chain_client
                    .local_balance()
                    .await
                    .expect("Use sync_balance instead");
                let time_total = time_start.elapsed().as_micros();
                info!("Local balance obtained after {} us", time_total);
                println!("{}", balance);
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            SyncBalance { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Synchronize chain information");
                let time_start = Instant::now();
                let balance = chain_client.synchronize_from_validators().await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Chain balance synchronized after {} us", time_total);
                println!("{}", balance);
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            QueryValidators { chain_id } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                info!("Starting operation to query validators");
                let time_start = Instant::now();
                let committee = chain_client.local_committee().await.unwrap();
                let time_total = time_start.elapsed().as_micros();
                info!("Validators obtained after {} us", time_total);
                info!("{:?}", committee.validators());
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            command @ (SetValidator { .. } | RemoveValidator { .. } | Pricing { .. }) => {
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
                    .map(|c| match c.value() {
                        CertificateValue::ConfirmedBlock { executed_block, .. } => {
                            executed_block.messages.len()
                        }
                        CertificateValue::ValidatedBlock { .. } => 0,
                    })
                    .sum::<usize>();
                info!("Subscribed {} chains to new committees", n);

                // Create the new committee.
                let mut committee = chain_client.local_committee().await.unwrap();
                let mut pricing = committee.pricing().clone();
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
                    Pricing {
                        certificate,
                        fuel,
                        storage,
                        messages,
                    } => {
                        if let Some(certificate) = certificate {
                            pricing.certificate = certificate;
                        }
                        if let Some(fuel) = fuel {
                            pricing.fuel = fuel;
                        }
                        if let Some(storage) = storage {
                            pricing.storage = storage;
                        }
                        if let Some(messages) = messages {
                            pricing.messages = messages;
                        }
                        println!(
                            "Pricing:\n\
                            {:.2} base cost per block\n\
                            {:.2} per unit of fuel used in executing user operations and messages\n\
                            {:.2} per byte of operations and incoming messages\n\
                            {:.2} per byte of outgoing messages",
                            pricing.certificate, pricing.fuel, pricing.storage, pricing.messages
                        );
                        if certificate.is_none()
                            && fuel.is_none()
                            && storage.is_none()
                            && messages.is_none()
                        {
                            return Ok(());
                        }
                    }
                    _ => unreachable!(),
                }
                committee = Committee::new(validators, pricing);
                let certificate = chain_client.stage_new_committee(committee).await.unwrap();
                context.update_wallet_from_client(&mut chain_client).await;
                info!("Staging committee:\n{:?}", certificate);
                context.push_to_all_chains(&storage, &certificate).await;

                // Remove the old committee.
                let certificate = chain_client.finalize_committee().await.unwrap();
                context.update_wallet_from_client(&mut chain_client).await;
                info!("Finalizing committee:\n{:?}", certificate);
                context.push_to_all_chains(&storage, &certificate).await;

                let time_total = time_start.elapsed().as_micros();
                info!("Operations confirmed after {} us", time_total);
                context.save_wallet();
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
                let votes: Vec<_> = responses
                    .into_iter()
                    .filter_map(|message| {
                        deserialize_response(message).and_then(|response| {
                            response.info.manager.pending().and_then(|vote| {
                                let value = values.get(&vote.value.value_hash)?.clone();
                                vote.clone().with_value(value)
                            })
                        })
                    })
                    .collect();
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

            Service {
                chain_id,
                config,
                port,
            } => {
                let mut chain_client = context.make_chain_client(storage, chain_id);
                chain_client.synchronize_from_validators().await?;
                let service = NodeService::new(chain_client, config, port);
                service
                    .run(context, |context, client| {
                        Box::pin(async {
                            context.update_wallet_from_client(client).await;
                            context.save_wallet();
                        })
                    })
                    .await?;
            }

            PublishBytecode {
                contract,
                service,
                publisher,
            } => {
                let start_time = Instant::now();
                let mut chain_client = context.make_chain_client(storage, publisher);
                let bytecode_id = context
                    .publish_bytecode(&mut chain_client, contract, service)
                    .await?;
                println!("{}", bytecode_id);
                info!("Time elapsed: {}s", start_time.elapsed().as_secs());
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
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
                let (application_id, _) = chain_client
                    .create_application_untyped(
                        bytecode_id,
                        parameters,
                        argument,
                        required_application_ids.unwrap_or_default(),
                    )
                    .await
                    .context("failed to create application")?;

                info!("{}", "Application created successfully!".green().bold());
                println!("{}", application_id);
                info!("Time elapsed: {}s", start_time.elapsed().as_secs());
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
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

                let bytecode_id = context
                    .publish_bytecode(&mut chain_client, contract, service)
                    .await?;

                // Saving wallet state in case the next step fails.
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();

                info!("Creating application...");
                let (application_id, _) = chain_client
                    .create_application_untyped(
                        bytecode_id,
                        parameters,
                        argument,
                        required_application_ids.unwrap_or_default(),
                    )
                    .await
                    .context("failed to create application")?;

                info!("{}", "Application published successfully!".green().bold());
                println!("{}", application_id);
                info!("Time elapsed: {}s", start_time.elapsed().as_secs());
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            RequestApplication {
                application_id,
                target_chain_id,
                requester_chain_id,
            } => {
                let mut chain_client = context.make_chain_client(storage, requester_chain_id);
                info!("Starting request");
                let certificate = chain_client
                    .request_application(application_id, target_chain_id)
                    .await
                    .unwrap();
                info!("{:?}", certificate);
                context.update_wallet_from_client(&mut chain_client).await;
                context.save_wallet();
            }

            Assign { key, message_id } => {
                let state = WorkerState::new("Local node".to_string(), None, storage)
                    .with_allow_inactive_chains(true)
                    .with_allow_messages_from_deprecated_epochs(true);
                let mut node_client = LocalNodeClient::new(state);

                // Take the latest committee we know of.
                let admin_chain_id = context.wallet_state.genesis_admin_chain();
                let query = ChainInfoQuery::new(admin_chain_id).with_committees();
                let info = node_client.handle_chain_info_query(query).await?;
                let Some(committee) = info.latest_committee() else {
                    bail!("Invalid chain info response; missing latest committee");
                };
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
                let timestamp = match certificate.value() {
                    CertificateValue::ConfirmedBlock {
                        executed_block: ExecutedBlock { block, .. },
                        ..
                    } => block.timestamp,
                    _ => panic!("Unexpected certificate."),
                };
                let chain_id = ChainId::child(message_id);
                context
                    .wallet_state
                    .assign_new_chain_to_key(key, chain_id, timestamp)
                    .context("could not assign the new chain")?;
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

                    let bytecode_id = context
                        .publish_bytecode(&mut chain_client, contract_path, service_path)
                        .await?;

                    // Saving wallet state in case the next step fails.
                    context.update_wallet_from_client(&mut chain_client).await;
                    context.save_wallet();

                    info!("Creating application...");
                    let (application_id, _) = chain_client
                        .create_application_untyped(
                            bytecode_id,
                            parameters,
                            argument,
                            required_application_ids.unwrap_or_default(),
                        )
                        .await
                        .context("failed to create application")?;

                    info!("{}", "Application published successfully!".green().bold());
                    println!("{}", application_id);
                    info!("Time elapsed: {}s", start_time.elapsed().as_secs());
                    context.update_wallet_from_client(&mut chain_client).await;
                    context.save_wallet();
                }
                _ => unreachable!("other project commands do not require storage"),
            },
            CreateGenesisConfig { .. } | Keygen | Wallet(_) | Net(_) => unreachable!(),
        }
        Ok(())
    }
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
            storage_price,
            messages_price,
        } => {
            let committee_config = CommitteeConfig::read(committee_config_path)
                .expect("Unable to read committee config file");
            let pricing = Pricing {
                certificate: *certificate_price,
                fuel: *fuel_price,
                storage: *storage_price,
                messages: *messages_price,
            };
            let mut genesis_config =
                GenesisConfig::new(committee_config, ChainId::root(*admin_root), pricing);
            let timestamp = start_timestamp
                .map(|st| {
                    Timestamp::from(
                        u64::try_from(st.timestamp()).expect("Start timestamp before 1970"),
                    )
                })
                .unwrap_or_else(Timestamp::now);
            let mut chains = vec![];
            for i in 0..*num {
                let description = ChainDescription::Root(i);
                // Create keys.
                let chain = UserChain::make_initial(description, timestamp);
                // Public "genesis" state.
                genesis_config.chains.push((
                    description,
                    chain.key_pair.as_ref().unwrap().public(),
                    *initial_funding,
                    timestamp,
                ));
                // Private keys.
                chains.push(chain);
            }
            let mut context = ClientContext::create(&options, genesis_config.clone(), chains)?;
            genesis_config.write(genesis_config_path)?;
            context.save_wallet();
            Ok(())
        }

        ClientCommand::Project(project_command) => match project_command {
            ProjectCommand::New { path } => {
                Project::new(path.clone())?;
                Ok(())
            }
            ProjectCommand::Test { path } => {
                let path = path.clone().unwrap_or_else(|| env::current_dir().unwrap());
                let project = Project::from_existing_project(path)?;
                Ok(project.test()?)
            }
            ProjectCommand::PublishAndCreate { .. } => run_command_with_storage(options).await,
        },

        ClientCommand::Keygen => {
            let mut context = ClientContext::from_options(&options)?;
            let key_pair = KeyPair::generate();
            let public = key_pair.public();
            context.wallet_state.add_unassigned_key_pair(key_pair);
            context.save_wallet();
            println!("{}", public);
            Ok(())
        }

        ClientCommand::Net(net_command) => match net_command {
            NetCommand::Up => {
                let network = Network::Grpc;
                let mut runner = LocalNet::new(network, 1);
                let client1 = runner.make_client(network);
                let client2 = runner.make_client(network);

                runner.generate_initial_validator_config().await;
                client1.create_genesis_config().await;
                client2.wallet_init(&[]).await;

                // Create initial server and client config.
                runner.run_local_net().await;
                let net_path = runner.net_path();

                println!(
                    "\nLinera net directory available at: {}",
                    net_path.display()
                );
                println!("To configure your Linera client for this network, run:\n");
                println!(
                    "{}",
                    format!(
                        "export LINERA_WALLET=\"{}\"",
                        net_path.join("wallet_0.json").display()
                    )
                    .bold()
                );
                println!(
                    "{}",
                    format!(
                        "export LINERA_STORAGE=\"rocksdb:{}\"",
                        net_path.join("linera.db").display()
                    )
                    .bold()
                );

                std::io::stdin().bytes().next();

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
                with_other_chains,
            } => {
                let genesis_config = GenesisConfig::read(genesis_config_path)?;
                let chains = with_other_chains
                    .iter()
                    .filter_map(|chain_id| {
                        let (description, _, _, timestamp) = genesis_config
                            .chains
                            .iter()
                            .find(|(desc, _, _, _)| ChainId::from(*desc) == *chain_id)?;
                        Some(UserChain::make_initial(*description, *timestamp))
                    })
                    .collect();
                let mut context = ClientContext::create(&options, genesis_config, chains)?;
                context.save_wallet();
                Ok(())
            }
        },

        _ => run_command_with_storage(options).await,
    }
}

async fn run_command_with_storage(options: ClientOptions) -> Result<(), Error> {
    let context = ClientContext::from_options(&options)?;
    let genesis_config = context.wallet_state.genesis_config().clone();
    let wasm_runtime = options.wasm_runtime.with_wasm_default();
    let cache_size = options.cache_size;
    let storage_config = ClientContext::storage_config(&options)?;

    storage_config
        .run_with_storage(
            &genesis_config,
            wasm_runtime,
            cache_size,
            Job(context, options.command),
        )
        .await?;
    Ok(())
}
