// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::ClientContext;
use anyhow::Error;
use chrono::{DateTime, Utc};
use linera_base::{
    crypto::PublicKey,
    data_types::Amount,
    identifiers::{BytecodeId, ChainId, MessageId},
};
use linera_execution::{
    committee::ValidatorName, system::Account, UserApplicationId, WasmRuntime, WithWasmDefault,
};
use linera_service::{
    chain_listener::{ChainListenerConfig, ClientContext as _},
    storage::{full_initialize_storage, run_with_storage},
    util,
};
use linera_views::common::CommonStoreConfig;
use std::{env, num::NonZeroU16, path::PathBuf, time::Duration};

use crate::Job;

#[derive(clap::Parser)]
#[command(
    name = "linera",
    version = clap::crate_version!(),
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
        let genesis_config = context.wallet_state().genesis_config().clone();
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
        let genesis_config = context.wallet_state().genesis_config().clone();
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
        /// Sending chain id (must be one of our chains)
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
        /// Chain id (must be one of our chains).
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

    /// Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one.
    OpenMultiOwnerChain {
        /// Chain id (must be one of our chains).
        #[arg(long = "from")]
        chain_id: Option<ChainId>,

        /// Public keys of the new owners
        #[arg(long = "to-public-keys", num_args(0..))]
        public_keys: Vec<PublicKey>,

        /// Weights for the new owners
        #[arg(long = "weights", num_args(0..))]
        weights: Vec<u64>,

        /// The number of rounds in which every owner can propose blocks, i.e. the first round
        /// number in which only a single designated leader is allowed to propose blocks.
        #[arg(long = "multi-leader-rounds")]
        multi_leader_rounds: Option<u32>,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[arg(long = "initial-balance", default_value = "0")]
        balance: Amount,

        /// The duration of the fast round, in milliseconds.
        #[arg(long = "fast-round-ms", value_parser = util::parse_millis)]
        fast_round_duration: Option<Duration>,

        /// The duration of the first single-leader and all multi-leader rounds.
        #[arg(
            long = "base-timeout-ms",
            default_value = "10000",
            value_parser = util::parse_millis
        )]
        base_timeout: Duration,

        /// The number of milliseconds by which the timeout increases after each
        /// single-leader round.
        #[arg(
            long = "timeout-increment-ms",
            default_value = "1000",
            value_parser = util::parse_millis
        )]
        timeout_increment: Duration,
    },

    /// Close (i.e. deactivate) an existing chain.
    CloseChain {
        /// Chain id (must be one of our chains)
        #[arg(long = "from")]
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
        #[arg(long, default_value = "0.001")]
        tokens_per_chain: Amount,
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
        /// The chain id to watch.
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

        /// The initialization argument as a JSON string.
        #[arg(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the initialization argument.
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

        /// The initialization argument as a JSON string.
        #[arg(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the initialization argument.
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
        #[arg(long, default_value = "10")]
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

        /// The initialization argument as a JSON string.
        #[arg(long)]
        json_argument: Option<String>,

        /// Path to a JSON file containing the initialization argument.
        #[arg(long)]
        json_argument_path: Option<PathBuf>,

        /// The list of required dependencies of application, if any.
        #[arg(long, num_args(0..))]
        required_application_ids: Option<Vec<UserApplicationId>>,
    },
}
