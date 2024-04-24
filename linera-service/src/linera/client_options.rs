// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{env, iter, num::NonZeroU16, path::PathBuf, time::Duration};

use anyhow::Error;
use chrono::{DateTime, Utc};
use linera_base::{
    crypto::PublicKey,
    data_types::{Amount, TimeDelta},
    identifiers::{Account, ApplicationId, BytecodeId, ChainId, MessageId, Owner},
    ownership::{ChainOwnership, TimeoutConfig},
};
use linera_core::client::MessagePolicy;
use linera_execution::{
    committee::ValidatorName, system::SystemChannel, UserApplicationId, WasmRuntime,
    WithWasmDefault,
};
use linera_service::{
    chain_listener::{ChainListenerConfig, ClientContext as _},
    storage::{full_initialize_storage, run_with_storage},
    util,
};
use linera_views::common::CommonStoreConfig;

use crate::{ClientContext, Job};

#[derive(clap::Parser)]
#[command(
    name = "linera",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "A Byzantine-fault tolerant sidechain with low-latency finality and high throughput",
)]
pub struct ClientOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
    #[arg(long = "wallet")]
    pub wallet_state_path: Option<PathBuf>,

    /// Storage configuration for the blockchain history.
    #[arg(long = "storage")]
    pub storage_config: Option<String>,

    /// Given an integer value N, read the wallet state and the wallet storage config from the
    /// environment variables LINERA_WALLET_{N} and LINERA_STORAGE_{N} instead of
    /// LINERA_WALLET and LINERA_STORAGE.
    #[arg(long, short = 'w')]
    pub with_wallet: Option<u32>,

    /// Timeout for sending queries (milliseconds)
    #[arg(long = "send-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub send_timeout: Duration,

    /// Timeout for receiving responses (milliseconds)
    #[arg(long = "recv-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub recv_timeout: Duration,

    #[arg(long, default_value = "10")]
    pub max_pending_messages: usize,

    /// The WebAssembly runtime to use.
    #[arg(long)]
    pub wasm_runtime: Option<WasmRuntime>,

    /// The maximal number of simultaneous queries to the database
    #[arg(long)]
    pub max_concurrent_queries: Option<usize>,

    /// The maximal number of simultaneous stream queries to the database
    #[arg(long, default_value = "10")]
    pub max_stream_queries: usize,

    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000")]
    pub cache_size: usize,

    /// Subcommand.
    #[command(subcommand)]
    pub command: ClientCommand,

    /// Delay increment for retrying to connect to a validator for notifications.
    #[arg(
        long = "notification-retry-delay-ms",
        default_value = "1000",
        value_parser = util::parse_millis
    )]
    pub notification_retry_delay: Duration,

    /// Number of times to retry connecting to a validator for notifications.
    #[arg(long, default_value = "10")]
    pub notification_retries: u32,

    /// Whether to wait until a quorum of validators has confirmed that all sent cross-chain
    /// messages have been delivered.
    #[arg(long)]
    pub wait_for_outgoing_messages: bool,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_CLIENT_TOKIO_THREADS")]
    pub tokio_threads: Option<usize>,

    /// The policy for handling incoming messages.
    #[arg(long, default_value = "accept")]
    pub message_policy: MessagePolicy,
}

impl ClientOptions {
    pub fn init() -> Result<Self, anyhow::Error> {
        let mut options = <ClientOptions as clap::Parser>::parse();
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

    pub async fn run_command_with_storage(self) -> Result<(), Error> {
        let context = ClientContext::from_options(&self)?;
        let genesis_config = context.wallet().genesis_config().clone();
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

    pub async fn initialize_storage(&self) -> Result<(), Error> {
        let context = ClientContext::from_options(self)?;
        let genesis_config = context.wallet().genesis_config().clone();
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

#[derive(Clone, clap::Subcommand)]
pub enum ClientCommand {
    /// Print CLI help in Markdown format, and exit.
    #[command(hide = true)]
    HelpMarkdown,

    /// Transfer funds
    Transfer {
        /// Sending chain ID (must be one of our chains)
        #[arg(long = "from")]
        sender: Account,

        /// Recipient account
        #[arg(long = "to")]
        recipient: Account,

        /// Amount to transfer
        amount: Amount,
    },

    /// Open (i.e. activate) a new chain deriving the UID from an existing one.
    OpenChain {
        /// Chain ID (must be one of our chains).
        #[arg(long = "from")]
        chain_id: Option<ChainId>,

        /// Public key of the new owner (otherwise create a key pair and remember it)
        #[arg(long = "to-public-key")]
        public_key: Option<PublicKey>,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[arg(long = "initial-balance", default_value = "0")]
        balance: Amount,
    },

    /// Subscribe to a system channel.
    Subscribe {
        /// Chain ID (must be one of our chains).
        #[arg(long)]
        subscriber: Option<ChainId>,

        /// Chain ID.
        #[arg(long)]
        publisher: Option<ChainId>,

        /// System channel available in the system application.
        #[arg(long)]
        channel: SystemChannel,
    },

    /// Unsubscribe from a system channel.
    Unsubscribe {
        /// Chain ID (must be one of our chains).
        #[arg(long)]
        subscriber: Option<ChainId>,

        /// Chain ID.
        #[arg(long)]
        publisher: Option<ChainId>,

        /// System channel available in the system application.
        #[arg(long)]
        channel: SystemChannel,
    },

    /// Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one.
    OpenMultiOwnerChain {
        /// Chain ID (must be one of our chains).
        #[arg(long = "from")]
        chain_id: Option<ChainId>,

        #[clap(flatten)]
        ownership_config: ChainOwnershipConfig,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[arg(long = "initial-balance", default_value = "0")]
        balance: Amount,
    },

    /// Change who owns the chain, and how the owners work together proposing blocks.
    ///
    /// Specify the complete set of new owners, by public key. Existing owners that are
    /// not included will be removed.
    ChangeOwnership {
        /// The ID of the chain whose owners will be changed.
        #[clap(long)]
        chain_id: Option<ChainId>,

        #[clap(flatten)]
        ownership_config: ChainOwnershipConfig,
    },

    /// Changes the application permissions configuration.
    ChangeApplicationPermissions {
        /// The ID of the chain to which the new permissions will be applied.
        #[arg(long)]
        chain_id: Option<ChainId>,
        /// If this is not set, all system operations and application operations are allowed.
        /// If it is set, only operations from the specified applications are allowed, and
        /// no system operations.
        #[arg(long)]
        execute_operations: Option<Vec<ApplicationId>>,
        /// These applications are allowed to close the current chain using the system API.
        #[arg(long)]
        close_chain: Vec<ApplicationId>,
    },

    /// Close an existing chain.
    ///
    /// A closed chain cannot execute operations or accept messages anymore.
    /// It can still reject incoming messages, so they bounce back to the sender.
    CloseChain {
        /// Chain ID (must be one of our chains)
        #[arg(long = "from")]
        chain_id: ChainId,
    },

    /// Read the current native-token balance of the given account directly from the local
    /// state.
    ///
    /// NOTE: The local balance does not reflect messages that are waiting to be picked in
    /// the local inbox, or that have not been synchronized from validators yet. Use
    /// `linera sync` then either `linera query-balance` or `linera process-inbox &&
    /// linera local-balance` for a consolidated balance.
    LocalBalance {
        /// The account to read, written as `CHAIN-ID:OWNER` or simply `CHAIN-ID` for the
        /// chain balance. By defaults, we read the chain balance of the default chain in
        /// the wallet.
        account: Option<Account>,
    },

    /// Simulate the execution of one block made of pending messages from the local inbox,
    /// then read the native-token balance of the account from the local state.
    ///
    /// NOTE: The balance does not reflect messages that have not been synchronized from
    /// validators yet. Call `linera sync` first to do so.
    QueryBalance {
        /// The account to query, written as `CHAIN-ID:OWNER` or simply `CHAIN-ID` for the
        /// chain balance. By defaults, we read the chain balance of the default chain in
        /// the wallet.
        account: Option<Account>,
    },

    /// (DEPRECATED) Synchronize the local state of the chain with a quorum validators, then query the
    /// local balance.
    ///
    /// This command is deprecated. Use `linera sync && linera query-balance` instead.
    SyncBalance {
        /// The account to query, written as `CHAIN-ID:OWNER` or simply `CHAIN-ID` for the
        /// chain balance. By defaults, we read the chain balance of the default chain in
        /// the wallet.
        account: Option<Account>,
    },

    /// Synchronize the local state of the chain with a quorum validators.
    Sync {
        /// The chain to synchronize with validators. If omitted, synchronizes the
        /// default chain of the wallet.
        chain_id: Option<ChainId>,
    },

    /// Process all pending incoming messages from the inbox of the given chain by creating as many
    /// blocks as needed to execute all (non-failing) messages. Failing messages will be
    /// marked as rejected and may bounce to their sender depending on their configuration.
    ProcessInbox {
        /// The chain to process. If omitted, uses the default chain of the wallet.
        chain_id: Option<ChainId>,
    },

    /// Show the current set of validators for a chain.
    QueryValidators {
        /// The chain to query. If omitted, query the default chain of the wallet.
        chain_id: Option<ChainId>,
    },

    /// Add or modify a validator (admin only)
    SetValidator {
        /// The public key of the validator.
        #[arg(long)]
        name: ValidatorName,

        /// Network address
        #[arg(long)]
        address: String,

        /// Voting power
        #[arg(long, default_value = "1")]
        votes: u64,
    },

    /// Remove a validator (admin only)
    RemoveValidator {
        /// The public key of the validator.
        #[arg(long)]
        name: ValidatorName,
    },

    /// View or update the resource control policy
    ResourceControlPolicy {
        /// Set the base price for creating a block.
        #[arg(long)]
        block: Option<Amount>,

        /// Set the price per unit of fuel.
        #[arg(long)]
        fuel_unit: Option<Amount>,

        /// Set the price per read operation.
        #[arg(long)]
        read_operation: Option<Amount>,

        /// Set the price per write operation.
        #[arg(long)]
        write_operation: Option<Amount>,

        /// Set the price per byte read.
        #[arg(long)]
        byte_read: Option<Amount>,

        /// Set the price per byte written.
        #[arg(long)]
        byte_written: Option<Amount>,

        /// Set the price per byte stored.
        #[arg(long)]
        byte_stored: Option<Amount>,

        /// Set the base price of sending a operation from a block..
        #[arg(long)]
        operation: Option<Amount>,

        /// Set the additional price for each byte in the argument of a user operation.
        #[arg(long)]
        operation_byte: Option<Amount>,

        /// Set the base price of sending a message from a block..
        #[arg(long)]
        message: Option<Amount>,

        /// Set the additional price for each byte in the argument of a user message.
        #[arg(long)]
        message_byte: Option<Amount>,

        /// Set the maximum read data per block.
        #[arg(long)]
        maximum_bytes_read_per_block: Option<u64>,

        /// Set the maximum write data per block.
        #[arg(long)]
        maximum_bytes_written_per_block: Option<u64>,
    },

    /// Send one transfer per chain in bulk mode
    #[cfg(feature = "benchmark")]
    Benchmark {
        /// Maximum number of blocks in flight
        #[arg(long, default_value = "200")]
        max_in_flight: usize,

        /// How many chains to use for the benchmark
        #[arg(long, default_value = "10")]
        num_chains: usize,

        /// How many tokens to assign to each newly created chain.
        /// These need to cover the transaction fees per chain for the benchmark.
        #[arg(long, default_value = "0.1")]
        tokens_per_chain: Amount,

        /// How many transactions to put in each block.
        #[arg(long, default_value = "1")]
        transactions_per_block: usize,

        /// The application ID of a fungible token on the wallet's default chain.
        /// If none is specified, the benchmark uses the native token.
        #[arg(long)]
        fungible_application_id: Option<linera_base::identifiers::ApplicationId>,
    },

    /// Create genesis configuration for a Linera deployment.
    /// Create initial user chains and print information to be used for initialization of validator setup.
    /// This will also create an initial wallet for the owner of the initial "root" chains.
    CreateGenesisConfig {
        /// Sets the file describing the public configurations of all validators
        #[arg(long = "committee")]
        committee_config_path: PathBuf,

        /// The output config path to be consumed by the server
        #[arg(long = "genesis")]
        genesis_config_path: PathBuf,

        /// Index of the admin chain in the genesis config
        #[arg(long, default_value = "0")]
        admin_root: u32,

        /// Known initial balance of the chain
        #[arg(long, default_value = "0")]
        initial_funding: Amount,

        /// The start timestamp: no blocks can be created before this time.
        #[arg(long)]
        start_timestamp: Option<DateTime<Utc>>,

        /// Number of initial (aka "root") chains to create in addition to the admin chain.
        num_other_initial_chains: u32,

        /// Set the base price for creating a block.
        #[arg(long, default_value = "0")]
        block_price: Amount,

        /// Set the price per unit of fuel.
        #[arg(long, default_value = "0")]
        fuel_unit_price: Amount,

        /// Set the price per read operation.
        #[arg(long, default_value = "0")]
        read_operation_price: Amount,

        /// Set the price per write operation.
        #[arg(long, default_value = "0")]
        write_operation_price: Amount,

        /// Set the price per byte read.
        #[arg(long, default_value = "0")]
        byte_read_price: Amount,

        /// Set the price per byte written.
        #[arg(long, default_value = "0")]
        byte_written_price: Amount,

        /// Set the price per byte stored.
        #[arg(long, default_value = "0")]
        byte_stored_price: Amount,

        /// Set the base price of sending a operation from a block..
        #[arg(long, default_value = "0")]
        operation_price: Amount,

        /// Set the additional price for each byte in the argument of a user operation.
        #[arg(long, default_value = "0")]
        operation_byte_price: Amount,

        /// Set the base price of sending a message from a block..
        #[arg(long, default_value = "0")]
        message_price: Amount,

        /// Set the additional price for each byte in the argument of a user message.
        #[arg(long, default_value = "0")]
        message_byte_price: Amount,

        /// Set the maximum read data per block.
        #[arg(long)]
        maximum_bytes_read_per_block: Option<u64>,

        /// Set the maximum write data per block.
        #[arg(long)]
        maximum_bytes_written_per_block: Option<u64>,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[arg(long)]
        testing_prng_seed: Option<u64>,

        /// A unique name to identify this network.
        #[arg(long)]
        network_name: Option<String>,
    },

    /// Watch the network for notifications.
    Watch {
        /// The chain ID to watch.
        chain_id: Option<ChainId>,

        /// Show all notifications from all validators.
        #[arg(long)]
        raw: bool,
    },

    /// Run a GraphQL service to explore and extend the chains of the wallet.
    Service {
        #[command(flatten)]
        config: ChainListenerConfig,

        /// The port on which to run the server
        #[arg(long = "port", default_value = "8080")]
        port: NonZeroU16,
    },

    /// Run a GraphQL service that exposes a faucet where users can claim tokens.
    /// This gives away the chain's tokens, and is mainly intended for testing.
    Faucet {
        /// The chain that gives away its tokens.
        chain_id: Option<ChainId>,

        /// The port on which to run the server
        #[arg(long = "port", default_value = "8080")]
        port: NonZeroU16,

        /// The number of tokens to send to each new chain.
        #[arg(long = "amount")]
        amount: Amount,

        /// The end timestamp: The faucet will rate-limit the token supply so it runs out of money
        /// no earlier than this.
        #[arg(long)]
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
        #[arg(long)]
        json_parameters: Option<String>,

        /// Path to a JSON file containing the shared parameters.
        #[arg(long)]
        json_parameters_path: Option<PathBuf>,

        /// The instantiation argument as a JSON string.
        #[arg(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the instantiation argument.
        #[arg(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[arg(long, num_args(0..))]
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
        #[arg(long)]
        json_parameters: Option<String>,

        /// Path to a JSON file containing the shared parameters.
        #[arg(long)]
        json_parameters_path: Option<PathBuf>,

        /// The instantiation argument as a JSON string.
        #[arg(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the instantiation argument.
        #[arg(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[arg(long, num_args(0..))]
        required_application_ids: Option<Vec<UserApplicationId>>,
    },

    /// Request an application from another chain, so it can be used on this one.
    RequestApplication {
        /// The ID of the application to request.
        application_id: UserApplicationId,

        /// The target chain on which the application is already registered.
        /// If not specified, the chain on which the application was created is used.
        #[arg(long)]
        target_chain_id: Option<ChainId>,

        /// The owned chain on which the application is missing.
        #[arg(long)]
        requester_chain_id: Option<ChainId>,
    },

    /// Create an unassigned key-pair.
    Keygen,

    /// Link a key owned by the wallet to a chain that was just created for that key.
    Assign {
        /// The public key to assign.
        #[arg(long)]
        key: PublicKey,

        /// The ID of the message that created the chain. (This uniquely describes the
        /// chain and where it was created.)
        #[arg(long)]
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
    #[command(subcommand)]
    Wallet(WalletCommand),

    /// Manage Linera projects.
    #[command(subcommand)]
    Project(ProjectCommand),

    /// Manage a local Linera Network.
    #[command(subcommand)]
    Net(NetCommand),
}

#[derive(Clone, clap::Parser)]
pub enum NetCommand {
    /// Start a Local Linera Network
    Up {
        /// The number of extra wallets and user chains to initialise. Default is 0.
        #[arg(long)]
        extra_wallets: Option<usize>,

        /// The number of initial "root" chains created in the genesis config on top of
        /// the default "admin" chain. All initial chains belong to the first "admin"
        /// wallet.
        #[arg(long, default_value = "10")]
        other_initial_chains: u32,

        /// The initial amount of native tokens credited in the initial "root" chains,
        /// including the default "admin" chain.
        #[arg(long, default_value = "1000000")]
        initial_amount: u128,

        /// The number of validators in the local test network. Default is 1.
        #[arg(long, default_value = "1")]
        validators: usize,

        /// The number of shards per validator in the local test network. Default is 1.
        #[arg(long, default_value = "1")]
        shards: usize,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[arg(long)]
        testing_prng_seed: Option<u64>,

        /// The name for the database table to store the chain data in.
        #[arg(long, default_value = "table_default")]
        table_name: String,

        /// Start the local network on a local Kubernetes deployment.
        #[cfg(feature = "kubernetes")]
        #[arg(long)]
        kubernetes: bool,

        /// If this is not set, we'll build the binaries from within the Docker container
        /// If it's set, but with no directory path arg, we'll look for the binaries based on `current_binary_parent`
        /// If it's set, but with a directory path arg, we'll get the binaries from that path directory
        #[cfg(feature = "kubernetes")]
        #[arg(long, num_args=0..=1)]
        binaries: Option<Option<PathBuf>>,
    },

    /// Print a bash helper script to make `linera net up` easier to use. The script is
    /// meant to be installed in `~/.bash_profile` or sourced when needed.
    Helper,
}

#[derive(Clone, clap::Subcommand)]
pub enum WalletCommand {
    /// Show the contents of the wallet.
    Show { chain_id: Option<ChainId> },

    /// Change the wallet default chain.
    SetDefault { chain_id: ChainId },

    /// Initialize a wallet from the genesis configuration.
    Init {
        /// The path to the genesis configuration for a Linera deployment. Either this or `--faucet`
        /// must be specified.
        #[arg(long = "genesis")]
        genesis_config_path: Option<PathBuf>,

        /// The address of a faucet.
        #[arg(long = "faucet")]
        faucet: Option<String>,

        /// Request a new chain from the faucet, credited with tokens. This requires `--faucet`.
        #[arg(long)]
        with_new_chain: bool,

        /// Other chains to follow.
        #[arg(long, num_args(0..))]
        with_other_chains: Vec<ChainId>,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[arg(long)]
        testing_prng_seed: Option<u64>,
    },

    /// Forgets the specified chain's keys.
    ForgetKeys { chain_id: ChainId },

    /// Forgets the specified chain, including the associated key pair.
    ForgetChain { chain_id: ChainId },
}

#[derive(Clone, clap::Parser)]
pub enum ProjectCommand {
    /// Create a new Linera project.
    New {
        /// The project name. A directory of the same name will be created in the current directory.
        name: String,

        /// Use the given clone of the Linera repository instead of remote crates.
        #[arg(long)]
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
        /// Defaults to the package name in Cargo.toml, with dashes replaced by
        /// underscores.
        name: Option<String>,

        /// An optional chain ID to publish the bytecode. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,

        /// The shared parameters as JSON string.
        #[arg(long)]
        json_parameters: Option<String>,

        /// Path to a JSON file containing the shared parameters.
        #[arg(long)]
        json_parameters_path: Option<PathBuf>,

        /// The instantiation argument as a JSON string.
        #[arg(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the instantiation argument.
        #[arg(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[arg(long, num_args(0..))]
        required_application_ids: Option<Vec<UserApplicationId>>,
    },
}

#[derive(Debug, Clone, clap::Args)]
pub struct ChainOwnershipConfig {
    /// Public keys of the new super owners.
    #[arg(long, num_args(0..))]
    super_owner_public_keys: Vec<PublicKey>,

    /// Public keys of the new regular owners.
    #[arg(long, num_args(0..))]
    owner_public_keys: Vec<PublicKey>,

    /// Weights for the new owners.
    ///
    /// If they are specified there must be exactly one weight for each owner.
    /// If no weights are given, every owner will have weight 100.
    #[arg(long, num_args(0..))]
    owner_weights: Vec<u64>,

    /// The number of rounds in which every owner can propose blocks, i.e. the first round
    /// number in which only a single designated leader is allowed to propose blocks.
    #[arg(long)]
    multi_leader_rounds: Option<u32>,

    /// The duration of the fast round, in milliseconds.
    #[arg(long = "fast-round-ms", value_parser = util::parse_millis_delta)]
    fast_round_duration: Option<TimeDelta>,

    /// The duration of the first single-leader and all multi-leader rounds.
    #[arg(
        long = "base-timeout-ms",
        default_value = "10000",
        value_parser = util::parse_millis_delta
    )]
    base_timeout: TimeDelta,

    /// The number of milliseconds by which the timeout increases after each
    /// single-leader round.
    #[arg(
        long = "timeout-increment-ms",
        default_value = "1000",
        value_parser = util::parse_millis_delta
    )]
    timeout_increment: TimeDelta,

    /// The age of an incoming tracked or protected message after which the validators start
    /// transitioning the chain to fallback mode, in milliseconds.
    #[arg(
        long = "fallback-duration-ms",
        default_value = "86400000", // 1 day
        value_parser = util::parse_millis_delta
    )]
    pub fallback_duration: TimeDelta,
}

impl TryFrom<ChainOwnershipConfig> for ChainOwnership {
    type Error = Error;

    fn try_from(config: ChainOwnershipConfig) -> Result<ChainOwnership, Error> {
        let ChainOwnershipConfig {
            super_owner_public_keys,
            owner_public_keys,
            owner_weights,
            multi_leader_rounds,
            fast_round_duration,
            base_timeout,
            timeout_increment,
            fallback_duration,
        } = config;
        anyhow::ensure!(
            owner_weights.is_empty() || owner_weights.len() == owner_public_keys.len(),
            "There are {} public keys but {} weights.",
            owner_public_keys.len(),
            owner_weights.len()
        );
        let super_owners = super_owner_public_keys
            .into_iter()
            .map(|pub_key| (Owner::from(pub_key), pub_key))
            .collect();
        let owners = owner_public_keys
            .into_iter()
            .zip(owner_weights.into_iter().chain(iter::repeat(100)))
            .map(|(pub_key, weight)| (Owner::from(pub_key), (pub_key, weight)))
            .collect();
        let multi_leader_rounds = multi_leader_rounds.unwrap_or(u32::MAX);
        let timeout_config = TimeoutConfig {
            fast_round_duration,
            base_timeout,
            timeout_increment,
            fallback_duration,
        };
        Ok(ChainOwnership {
            super_owners,
            owners,
            multi_leader_rounds,
            timeout_config,
        })
    }
}
