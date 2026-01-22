// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{borrow::Cow, num::NonZeroU16, path::PathBuf};

use chrono::{DateTime, Utc};
use linera_base::{
    crypto::{AccountPublicKey, CryptoHash, ValidatorPublicKey},
    data_types::{Amount, BlockHeight, Epoch},
    identifiers::{Account, AccountOwner, ApplicationId, ChainId, ModuleId, StreamId},
    time::Duration,
    vm::VmRuntime,
};
use linera_client::{
    chain_listener::ChainListenerConfig,
    client_options::{
        ApplicationPermissionsConfig, ChainOwnershipConfig, ResourceControlPolicyConfig,
    },
    util,
};
use linera_rpc::config::CrossChainConfig;

use crate::{cli::validator, task_processor::parse_operator};

const DEFAULT_TOKENS_PER_CHAIN: Amount = Amount::from_millis(100);
const DEFAULT_TRANSACTIONS_PER_BLOCK: usize = 1;
const DEFAULT_WRAP_UP_MAX_IN_FLIGHT: usize = 5;
const DEFAULT_NUM_CHAINS: usize = 10;
const DEFAULT_BPS: usize = 10;

/// Specification for a validator to be added to the committee.
#[derive(Clone, Debug)]
pub struct ValidatorToAdd {
    pub public_key: ValidatorPublicKey,
    pub account_key: AccountPublicKey,
    pub address: String,
    pub votes: u64,
}

impl std::str::FromStr for ValidatorToAdd {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.split(',').collect();
        anyhow::ensure!(
            parts.len() == 4,
            "Validator spec must be in format: public_key,account_key,address,votes"
        );

        Ok(ValidatorToAdd {
            public_key: parts[0].parse()?,
            account_key: parts[1].parse()?,
            address: parts[2].to_string(),
            votes: parts[3].parse()?,
        })
    }
}

#[derive(Clone, clap::Args, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct BenchmarkOptions {
    /// How many chains to use.
    #[arg(long, default_value_t = DEFAULT_NUM_CHAINS)]
    pub num_chains: usize,

    /// How many tokens to assign to each newly created chain.
    /// These need to cover the transaction fees per chain for the benchmark.
    #[arg(long, default_value_t = DEFAULT_TOKENS_PER_CHAIN)]
    pub tokens_per_chain: Amount,

    /// How many transactions to put in each block.
    #[arg(long, default_value_t = DEFAULT_TRANSACTIONS_PER_BLOCK)]
    pub transactions_per_block: usize,

    /// The application ID of a fungible token on the wallet's default chain.
    /// If none is specified, the benchmark uses the native token.
    #[arg(long)]
    pub fungible_application_id: Option<linera_base::identifiers::ApplicationId>,

    /// The fixed BPS (Blocks Per Second) rate that block proposals will be sent at.
    #[arg(long, default_value_t = DEFAULT_BPS)]
    pub bps: usize,

    /// If provided, will close the chains after the benchmark is finished. Keep in mind that
    /// closing the chains might take a while, and will increase the validator latency while
    /// they're being closed.
    #[arg(long)]
    pub close_chains: bool,

    /// A comma-separated list of host:port pairs to query for health metrics.
    /// If provided, the benchmark will check these endpoints for validator health
    /// and terminate if any validator is unhealthy.
    /// Example: "127.0.0.1:21100,validator-1.some-network.linera.net:21100"
    #[arg(long)]
    pub health_check_endpoints: Option<String>,

    /// The maximum number of in-flight requests to validators when wrapping up the benchmark.
    /// While wrapping up, this controls the concurrency level when processing inboxes and
    /// closing chains.
    #[arg(long, default_value_t = DEFAULT_WRAP_UP_MAX_IN_FLIGHT)]
    pub wrap_up_max_in_flight: usize,

    /// Confirm before starting the benchmark.
    #[arg(long)]
    pub confirm_before_start: bool,

    /// How long to run the benchmark for. If not provided, the benchmark will run until
    /// it is interrupted.
    #[arg(long)]
    pub runtime_in_seconds: Option<u64>,

    /// The delay between chains, in milliseconds. For example, if set to 200ms, the first
    /// chain will start, then the second will start 200 ms after the first one, the third
    /// 200 ms after the second one, and so on.
    /// This is used for slowly ramping up the TPS, so we don't pound the validators with the full
    /// TPS all at once.
    #[arg(long)]
    pub delay_between_chains_ms: Option<u64>,

    /// Path to YAML file containing chain IDs to send transfers to.
    /// If not provided, only transfers between chains in the same wallet.
    #[arg(long)]
    pub config_path: Option<PathBuf>,

    /// Transaction distribution mode. If false (default), distributes transactions evenly
    /// across chains within each block. If true, sends all transactions in each block
    /// to a single chain, rotating through chains for subsequent blocks.
    #[arg(long)]
    pub single_destination_per_block: bool,
}

impl Default for BenchmarkOptions {
    fn default() -> Self {
        Self {
            num_chains: DEFAULT_NUM_CHAINS,
            tokens_per_chain: DEFAULT_TOKENS_PER_CHAIN,
            transactions_per_block: DEFAULT_TRANSACTIONS_PER_BLOCK,
            wrap_up_max_in_flight: DEFAULT_WRAP_UP_MAX_IN_FLIGHT,
            fungible_application_id: None,
            bps: DEFAULT_BPS,
            close_chains: false,
            health_check_endpoints: None,
            confirm_before_start: false,
            runtime_in_seconds: None,
            delay_between_chains_ms: None,
            config_path: None,
            single_destination_per_block: false,
        }
    }
}

#[derive(Clone, clap::Subcommand, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum BenchmarkCommand {
    /// Start a single benchmark process, maintaining a given TPS.
    Single {
        #[command(flatten)]
        options: BenchmarkOptions,
    },

    /// Run multiple benchmark processes in parallel.
    Multi {
        #[command(flatten)]
        options: BenchmarkOptions,

        /// The number of benchmark processes to run in parallel.
        #[arg(long, default_value = "1")]
        processes: usize,

        /// The faucet (which implicitly defines the network)
        #[arg(long, env = "LINERA_FAUCET_URL")]
        faucet: String,

        /// If specified, a directory with a random name will be created in this directory, and the
        /// client state will be stored there.
        /// If not specified, a temporary directory will be used for each client.
        #[arg(long)]
        client_state_dir: Option<String>,

        /// The delay between starting the benchmark processes, in seconds.
        /// If --cross-wallet-transfers is true, this will be ignored.
        #[arg(long, default_value = "10")]
        delay_between_processes: u64,

        /// Whether to send transfers between chains in different wallets.
        #[arg(long)]
        cross_wallet_transfers: bool,
    },
}

impl BenchmarkCommand {
    pub fn transactions_per_block(&self) -> usize {
        match self {
            Self::Single { options } => options.transactions_per_block,
            Self::Multi { options, .. } => options.transactions_per_block,
        }
    }
}

#[cfg(feature = "kubernetes")]
use crate::cli_wrappers::local_kubernetes_net::BuildMode;
use crate::util::{
    DEFAULT_PAUSE_AFTER_GQL_MUTATIONS_SECS, DEFAULT_PAUSE_AFTER_LINERA_SERVICE_SECS,
};

#[derive(Clone, clap::Subcommand)]
pub enum ClientCommand {
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

        /// The new owner (otherwise create a key pair and remember it)
        #[arg(long = "owner")]
        owner: Option<AccountOwner>,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[arg(long = "initial-balance", default_value = "0")]
        balance: Amount,

        /// Whether to create a super owner for the new chain.
        #[arg(long)]
        super_owner: bool,
    },

    /// Open (i.e. activate) a new multi-owner chain deriving the UID from an existing one.
    OpenMultiOwnerChain {
        /// Chain ID (must be one of our chains).
        #[arg(long = "from")]
        chain_id: Option<ChainId>,

        #[clap(flatten)]
        ownership_config: ChainOwnershipConfig,

        #[clap(flatten)]
        application_permissions_config: ApplicationPermissionsConfig,

        /// The initial balance of the new chain. This is subtracted from the parent chain's
        /// balance.
        #[arg(long = "initial-balance", default_value = "0")]
        balance: Amount,
    },

    /// Display who owns the chain, and how the owners work together proposing blocks.
    ShowOwnership {
        /// The ID of the chain whose owners will be changed.
        #[clap(long)]
        chain_id: Option<ChainId>,
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

    /// Change the preferred owner of a chain.
    SetPreferredOwner {
        /// The ID of the chain whose preferred owner will be changed.
        #[clap(long)]
        chain_id: Option<ChainId>,

        /// The new preferred owner.
        #[arg(long)]
        owner: AccountOwner,
    },

    /// Changes the application permissions configuration.
    ChangeApplicationPermissions {
        /// The ID of the chain to which the new permissions will be applied.
        #[arg(long)]
        chain_id: Option<ChainId>,

        #[clap(flatten)]
        application_permissions_config: ApplicationPermissionsConfig,
    },

    /// Close an existing chain.
    ///
    /// A closed chain cannot execute operations or accept messages anymore.
    /// It can still reject incoming messages, so they bounce back to the sender.
    CloseChain {
        /// Chain ID (must be one of our chains)
        chain_id: ChainId,
    },

    /// Print out the network description.
    ShowNetworkDescription,

    /// Read the current native-token balance of the given account directly from the local
    /// state.
    ///
    /// NOTE: The local balance does not reflect messages that are waiting to be picked in
    /// the local inbox, or that have not been synchronized from validators yet. Use
    /// `linera sync` then either `linera query-balance` or `linera process-inbox &&
    /// linera local-balance` for a consolidated balance.
    LocalBalance {
        /// The account to read, written as `OWNER@CHAIN-ID` or simply `CHAIN-ID` for the
        /// chain balance. By default, we read the chain balance of the default chain in
        /// the wallet.
        account: Option<Account>,
    },

    /// Simulate the execution of one block made of pending messages from the local inbox,
    /// then read the native-token balance of the account from the local state.
    ///
    /// NOTE: The balance does not reflect messages that have not been synchronized from
    /// validators yet. Call `linera sync` first to do so.
    QueryBalance {
        /// The account to query, written as `OWNER@CHAIN-ID` or simply `CHAIN-ID` for the
        /// chain balance. By default, we read the chain balance of the default chain in
        /// the wallet.
        account: Option<Account>,
    },

    /// (DEPRECATED) Synchronize the local state of the chain with a quorum validators, then query the
    /// local balance.
    ///
    /// This command is deprecated. Use `linera sync && linera query-balance` instead.
    SyncBalance {
        /// The account to query, written as `OWNER@CHAIN-ID` or simply `CHAIN-ID` for the
        /// chain balance. By default, we read the chain balance of the default chain in
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

    /// Query validators for shard information about a specific chain.
    QueryShardInfo {
        /// The chain to query shard information for.
        chain_id: ChainId,
    },

    /// Deprecates all committees up to and including the specified one.
    RevokeEpochs { epoch: Epoch },

    /// View or update the resource control policy
    ResourceControlPolicy {
        /// Set the price per unit of Wasm fuel.
        #[arg(long)]
        wasm_fuel_unit: Option<Amount>,

        /// Set the price per unit of EVM fuel.
        #[arg(long)]
        evm_fuel_unit: Option<Amount>,

        /// Set the price per read operation.
        #[arg(long)]
        read_operation: Option<Amount>,

        /// Set the price per write operation.
        #[arg(long)]
        write_operation: Option<Amount>,

        /// Set the price per byte read from runtime.
        #[arg(long)]
        byte_runtime: Option<Amount>,

        /// Set the price per byte read.
        #[arg(long)]
        byte_read: Option<Amount>,

        /// Set the price per byte written.
        #[arg(long)]
        byte_written: Option<Amount>,

        /// Set the base price to read a blob.
        #[arg(long)]
        blob_read: Option<Amount>,

        /// Set the base price to publish a blob.
        #[arg(long)]
        blob_published: Option<Amount>,

        /// Set the price to read a blob, per byte.
        #[arg(long)]
        blob_byte_read: Option<Amount>,

        /// The price to publish a blob, per byte.
        #[arg(long)]
        blob_byte_published: Option<Amount>,

        /// Set the price per byte stored.
        #[arg(long)]
        byte_stored: Option<Amount>,

        /// Set the base price of sending an operation from a block..
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

        /// Set the price per query to a service as an oracle.
        #[arg(long)]
        service_as_oracle_query: Option<Amount>,

        /// Set the price for performing an HTTP request.
        #[arg(long)]
        http_request: Option<Amount>,

        /// Set the maximum amount of Wasm fuel per block.
        #[arg(long)]
        maximum_wasm_fuel_per_block: Option<u64>,

        /// Set the maximum amount of EVM fuel per block.
        #[arg(long)]
        maximum_evm_fuel_per_block: Option<u64>,

        /// Set the maximum time in milliseconds that a block can spend executing services as oracles.
        #[arg(long)]
        maximum_service_oracle_execution_ms: Option<u64>,

        /// Set the maximum size of a block, in bytes.
        #[arg(long)]
        maximum_block_size: Option<u64>,

        /// Set the maximum size of data blobs, compressed bytecode and other binary blobs,
        /// in bytes.
        #[arg(long)]
        maximum_blob_size: Option<u64>,

        /// Set the maximum number of published blobs per block.
        #[arg(long)]
        maximum_published_blobs: Option<u64>,

        /// Set the maximum size of decompressed contract or service bytecode, in bytes.
        #[arg(long)]
        maximum_bytecode_size: Option<u64>,

        /// Set the maximum size of a block proposal, in bytes.
        #[arg(long)]
        maximum_block_proposal_size: Option<u64>,

        /// Set the maximum read data per block.
        #[arg(long)]
        maximum_bytes_read_per_block: Option<u64>,

        /// Set the maximum write data per block.
        #[arg(long)]
        maximum_bytes_written_per_block: Option<u64>,

        /// Set the maximum size of oracle responses.
        #[arg(long)]
        maximum_oracle_response_bytes: Option<u64>,

        /// Set the maximum size in bytes of a received HTTP response.
        #[arg(long)]
        maximum_http_response_bytes: Option<u64>,

        /// Set the maximum amount of time allowed to wait for an HTTP response.
        #[arg(long)]
        http_request_timeout_ms: Option<u64>,

        /// Set the list of hosts that contracts and services can send HTTP requests to.
        #[arg(long)]
        http_request_allow_list: Option<Vec<String>>,
    },

    /// Run benchmarks to test network performance.
    #[command(subcommand)]
    Benchmark(BenchmarkCommand),

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

        /// Known initial balance of the chain
        #[arg(long, default_value = "0")]
        initial_funding: Amount,

        /// The start timestamp: no blocks can be created before this time.
        #[arg(long)]
        start_timestamp: Option<DateTime<Utc>>,

        /// Number of initial (aka "root") chains to create in addition to the admin chain.
        num_other_initial_chains: u32,

        /// Configure the resource control policy (notably fees) according to pre-defined
        /// settings.
        #[arg(long, default_value = "no-fees")]
        policy_config: ResourceControlPolicyConfig,

        /// Set the price per unit of Wasm fuel.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        wasm_fuel_unit_price: Option<Amount>,

        /// Set the price per unit of EVM fuel.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        evm_fuel_unit_price: Option<Amount>,

        /// Set the price per read operation.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        read_operation_price: Option<Amount>,

        /// Set the price per write operation.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        write_operation_price: Option<Amount>,

        /// Set the price per byte read from runtime.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        byte_runtime_price: Option<Amount>,

        /// Set the price per byte read.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        byte_read_price: Option<Amount>,

        /// Set the price per byte written.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        byte_written_price: Option<Amount>,

        /// Set the base price to read a blob.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        blob_read_price: Option<Amount>,

        /// Set the base price to publish a blob.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        blob_published_price: Option<Amount>,

        /// Set the price to read a blob, per byte.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        blob_byte_read_price: Option<Amount>,

        /// Set the price to publish a blob, per byte.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        blob_byte_published_price: Option<Amount>,

        /// Set the price per byte stored.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        byte_stored_price: Option<Amount>,

        /// Set the base price of sending an operation from a block..
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        operation_price: Option<Amount>,

        /// Set the additional price for each byte in the argument of a user operation.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        operation_byte_price: Option<Amount>,

        /// Set the base price of sending a message from a block..
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        message_price: Option<Amount>,

        /// Set the additional price for each byte in the argument of a user message.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        message_byte_price: Option<Amount>,

        /// Set the price per query to a service as an oracle.
        #[arg(long)]
        service_as_oracle_query_price: Option<Amount>,

        /// Set the price for performing an HTTP request.
        #[arg(long)]
        http_request_price: Option<Amount>,

        /// Set the maximum amount of Wasm fuel per block.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_wasm_fuel_per_block: Option<u64>,

        /// Set the maximum amount of EVM fuel per block.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_evm_fuel_per_block: Option<u64>,

        /// Set the maximum time in milliseconds that a block can spend executing services as oracles.
        #[arg(long)]
        maximum_service_oracle_execution_ms: Option<u64>,

        /// Set the maximum size of a block.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_block_size: Option<u64>,

        /// Set the maximum size of decompressed contract or service bytecode, in bytes.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_bytecode_size: Option<u64>,

        /// Set the maximum size of data blobs, compressed bytecode and other binary blobs,
        /// in bytes.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_blob_size: Option<u64>,

        /// Set the maximum number of published blobs per block.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_published_blobs: Option<u64>,

        /// Set the maximum size of a block proposal, in bytes.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_block_proposal_size: Option<u64>,

        /// Set the maximum read data per block.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_bytes_read_per_block: Option<u64>,

        /// Set the maximum write data per block.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_bytes_written_per_block: Option<u64>,

        /// Set the maximum size of oracle responses.
        /// (This will overwrite value from `--policy-config`)
        #[arg(long)]
        maximum_oracle_response_bytes: Option<u64>,

        /// Set the maximum size in bytes of a received HTTP response.
        #[arg(long)]
        maximum_http_response_bytes: Option<u64>,

        /// Set the maximum amount of time allowed to wait for an HTTP response.
        #[arg(long)]
        http_request_timeout_ms: Option<u64>,

        /// Set the list of hosts that contracts and services can send HTTP requests to.
        #[arg(long)]
        http_request_allow_list: Option<Vec<String>>,

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
        #[arg(long)]
        port: NonZeroU16,

        /// The port to expose metrics on.
        #[cfg(with_metrics)]
        #[arg(long)]
        metrics_port: NonZeroU16,

        /// Application IDs of operator applications to watch.
        /// When specified, a task processor is started alongside the node service.
        #[arg(long = "operator-application-ids")]
        operator_application_ids: Vec<ApplicationId>,

        /// A controller to execute a dynamic set of applications running on a dynamic set of
        /// chains.
        #[arg(long = "controller-id")]
        controller_application_id: Option<ApplicationId>,

        /// Supported operators and their binary paths.
        /// Format: `name=path` or just `name` (uses name as path).
        /// Example: `--operators my-operator=/path/to/binary`
        #[arg(long = "operators", value_parser = parse_operator)]
        operators: Vec<(String, PathBuf)>,

        /// Run in read-only mode: disallow mutations and prevent queries from scheduling
        /// operations. Use this when exposing the service to untrusted clients.
        #[arg(long)]
        read_only: bool,
    },

    /// Run a GraphQL service that exposes a faucet where users can claim tokens.
    /// This gives away the chain's tokens, and is mainly intended for testing.
    Faucet {
        /// The chain that gives away its tokens.
        chain_id: Option<ChainId>,

        /// The port on which to run the server
        #[arg(long, default_value = "8080")]
        port: u16,

        /// The port for prometheus to scrape.
        #[cfg(with_metrics)]
        #[arg(long, default_value = "9090")]
        metrics_port: u16,

        /// The number of tokens to send to each new chain.
        #[arg(long)]
        amount: Amount,

        /// The end timestamp: The faucet will rate-limit the token supply so it runs out of money
        /// no earlier than this.
        #[arg(long)]
        limit_rate_until: Option<DateTime<Utc>>,

        /// Configuration for the faucet chain listener.
        #[command(flatten)]
        config: ChainListenerConfig,

        /// Path to the persistent storage file for faucet mappings.
        #[arg(long)]
        storage_path: PathBuf,

        /// Maximum number of operations to include in a single block (default: 100).
        #[arg(long, default_value = "100")]
        max_batch_size: usize,
    },

    /// Publish module.
    PublishModule {
        /// Path to the Wasm file for the application "contract" bytecode.
        contract: PathBuf,

        /// Path to the Wasm file for the application "service" bytecode.
        service: PathBuf,

        /// The virtual machine runtime to use.
        #[arg(long, default_value = "wasm")]
        vm_runtime: VmRuntime,

        /// An optional chain ID to publish the module. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,
    },

    /// Print events from a specific chain and stream from a specified index.
    ListEventsFromIndex {
        /// The chain to query. If omitted, query the default chain of the wallet.
        chain_id: Option<ChainId>,

        /// The stream being considered.
        #[arg(long)]
        stream_id: StreamId,

        /// Index of the message to start with
        #[arg(long, default_value = "0")]
        start_index: u32,
    },

    /// Publish a data blob of binary data.
    PublishDataBlob {
        /// Path to data blob file to be published.
        blob_path: PathBuf,
        /// An optional chain ID to publish the blob. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,
    },

    // TODO(#2490): Consider removing or renaming this.
    /// Verify that a data blob is readable.
    ReadDataBlob {
        /// The hash of the content.
        hash: CryptoHash,
        /// An optional chain ID to verify the blob. The default chain of the wallet
        /// is used otherwise.
        reader: Option<ChainId>,
    },

    /// Create an application.
    CreateApplication {
        /// The module ID of the application to create.
        module_id: ModuleId,

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
        required_application_ids: Option<Vec<ApplicationId>>,
    },

    /// Create an application, and publish the required module.
    PublishAndCreate {
        /// Path to the Wasm file for the application "contract" bytecode.
        contract: PathBuf,

        /// Path to the Wasm file for the application "service" bytecode.
        service: PathBuf,

        /// The virtual machine runtime to use.
        #[arg(long, default_value = "wasm")]
        vm_runtime: VmRuntime,

        /// An optional chain ID to publish the module. The default chain of the wallet
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
        required_application_ids: Option<Vec<ApplicationId>>,
    },

    /// Create an unassigned key pair.
    Keygen,

    /// Link the owner to the chain.
    /// Expects that the caller has a private key corresponding to the `public_key`,
    /// otherwise block proposals will fail when signing with it.
    Assign {
        /// The owner to assign.
        #[arg(long)]
        owner: AccountOwner,

        /// The ID of the chain.
        #[arg(long)]
        chain_id: ChainId,
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

    /// Show the information about a chain.
    #[command(subcommand)]
    Chain(ChainCommand),

    /// Manage Linera projects.
    #[command(subcommand)]
    Project(ProjectCommand),

    /// Manage a local Linera Network.
    #[command(subcommand)]
    Net(NetCommand),

    /// Manage validators in the committee.
    #[command(subcommand)]
    Validator(validator::Command),

    /// Operation on the storage.
    #[command(subcommand)]
    Storage(DatabaseToolCommand),

    /// Print CLI help in Markdown format, and exit.
    #[command(hide = true)]
    HelpMarkdown,

    /// Extract a Bash and GraphQL script embedded in a markdown file and print it on
    /// `stdout`.
    #[command(hide = true)]
    ExtractScriptFromMarkdown {
        /// The source file
        path: PathBuf,

        /// Insert a pause of N seconds after calls to `linera service`.
        #[arg(long, default_value = DEFAULT_PAUSE_AFTER_LINERA_SERVICE_SECS, value_parser = util::parse_secs)]
        pause_after_linera_service: Duration,

        /// Insert a pause of N seconds after GraphQL queries.
        #[arg(long, default_value = DEFAULT_PAUSE_AFTER_GQL_MUTATIONS_SECS, value_parser = util::parse_secs)]
        pause_after_gql_mutations: Duration,
    },

    /// Generate shell completion scripts
    Completion {
        /// The shell to generate completions for
        #[arg(value_enum)]
        shell: clap_complete::Shell,
    },
}

impl ClientCommand {
    /// Returns the log file name to use based on the [`ClientCommand`] that will run.
    pub fn log_file_name(&self) -> Cow<'static, str> {
        match self {
            ClientCommand::Transfer { .. }
            | ClientCommand::OpenChain { .. }
            | ClientCommand::OpenMultiOwnerChain { .. }
            | ClientCommand::ShowOwnership { .. }
            | ClientCommand::ChangeOwnership { .. }
            | ClientCommand::SetPreferredOwner { .. }
            | ClientCommand::ChangeApplicationPermissions { .. }
            | ClientCommand::CloseChain { .. }
            | ClientCommand::ShowNetworkDescription
            | ClientCommand::LocalBalance { .. }
            | ClientCommand::QueryBalance { .. }
            | ClientCommand::SyncBalance { .. }
            | ClientCommand::Sync { .. }
            | ClientCommand::ProcessInbox { .. }
            | ClientCommand::QueryShardInfo { .. }
            | ClientCommand::ResourceControlPolicy { .. }
            | ClientCommand::RevokeEpochs { .. }
            | ClientCommand::CreateGenesisConfig { .. }
            | ClientCommand::PublishModule { .. }
            | ClientCommand::ListEventsFromIndex { .. }
            | ClientCommand::PublishDataBlob { .. }
            | ClientCommand::ReadDataBlob { .. }
            | ClientCommand::CreateApplication { .. }
            | ClientCommand::PublishAndCreate { .. }
            | ClientCommand::Keygen
            | ClientCommand::Assign { .. }
            | ClientCommand::Wallet { .. }
            | ClientCommand::Chain { .. }
            | ClientCommand::Validator { .. }
            | ClientCommand::RetryPendingBlock { .. } => "client".into(),
            ClientCommand::Benchmark(BenchmarkCommand::Single { .. }) => "single-benchmark".into(),
            ClientCommand::Benchmark(BenchmarkCommand::Multi { .. }) => "multi-benchmark".into(),
            ClientCommand::Net { .. } => "net".into(),
            ClientCommand::Project { .. } => "project".into(),
            ClientCommand::Watch { .. } => "watch".into(),
            ClientCommand::Storage { .. } => "storage".into(),
            ClientCommand::Service { port, .. } => format!("service-{port}").into(),
            ClientCommand::Faucet { .. } => "faucet".into(),
            ClientCommand::HelpMarkdown
            | ClientCommand::ExtractScriptFromMarkdown { .. }
            | ClientCommand::Completion { .. } => "tool".into(),
        }
    }
}

#[derive(Clone, clap::Parser)]
pub enum DatabaseToolCommand {
    /// Delete all the namespaces in the database
    DeleteAll,

    /// Delete a single namespace from the database
    DeleteNamespace,

    /// Check existence of a namespace in the database
    CheckExistence,

    /// Initialize a namespace in the database
    Initialize {
        #[arg(long = "genesis")]
        genesis_config_path: PathBuf,
    },

    /// List the namespaces in the database
    ListNamespaces,

    /// List the blob IDs in the database
    ListBlobIds,

    /// List the chain IDs in the database
    ListChainIds,

    /// List the event IDs in the database
    ListEventIds,
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, clap::Parser)]
pub enum NetCommand {
    /// Start a Local Linera Network
    Up {
        /// The number of initial "root" chains created in the genesis config on top of
        /// the default "admin" chain. All initial chains belong to the first "admin"
        /// wallet. It is recommended to use at least one other initial chain for the
        /// faucet.
        #[arg(long, default_value = "2")]
        other_initial_chains: u32,

        /// The initial amount of native tokens credited in the initial "root" chains,
        /// including the default "admin" chain.
        #[arg(long, default_value = "1000000")]
        initial_amount: u128,

        /// The number of validators in the local test network.
        #[arg(long, default_value = "1")]
        validators: usize,

        /// The number of proxies in the local test network.
        #[arg(long, default_value = "1")]
        proxies: usize,

        /// The number of shards per validator in the local test network.
        #[arg(long, default_value = "1")]
        shards: usize,

        /// Configure the resource control policy (notably fees) according to pre-defined
        /// settings.
        #[arg(long, default_value = "no-fees")]
        policy_config: ResourceControlPolicyConfig,

        /// The configuration for cross-chain messages.
        #[clap(flatten)]
        cross_chain_config: CrossChainConfig,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[arg(long)]
        testing_prng_seed: Option<u64>,

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

        /// Don't build docker image. This assumes that the image is already built.
        #[cfg(feature = "kubernetes")]
        #[arg(long, default_value = "false")]
        no_build: bool,

        /// The name of the docker image to use.
        #[cfg(feature = "kubernetes")]
        #[arg(long, default_value = "linera:latest")]
        docker_image_name: String,

        /// The build mode to use.
        #[cfg(feature = "kubernetes")]
        #[arg(long, default_value = "release")]
        build_mode: BuildMode,

        /// Run with a specific path where the wallet and validator input files are.
        /// If none, then a temporary directory is created.
        #[arg(long)]
        path: Option<String>,

        /// External protocol used, either `grpc` or `grpcs`.
        #[arg(long, default_value = "grpc")]
        external_protocol: String,

        /// If present, a faucet is started using the chain provided by --faucet-chain, or
        /// the first non-admin chain if not provided.
        #[arg(long, default_value = "false")]
        with_faucet: bool,

        /// When using --with-faucet, this specifies the chain on which the faucet will be started.
        /// If this is `n`, the `n`-th non-admin chain (lexicographically) in the wallet is selected.
        #[arg(long)]
        faucet_chain: Option<u32>,

        /// The port on which to run the faucet server
        #[arg(long, default_value = "8080")]
        faucet_port: NonZeroU16,

        /// The number of tokens to send to each new chain created by the faucet.
        #[arg(long, default_value = "1000")]
        faucet_amount: Amount,

        /// Whether to start a block exporter for each validator.
        #[arg(long, default_value = "false")]
        with_block_exporter: bool,

        /// The number of block exporters to start.
        #[arg(long, default_value = "1")]
        num_block_exporters: usize,

        /// The address of the block exporter.
        #[arg(long, default_value = "localhost")]
        exporter_address: String,

        /// The port on which to run the block exporter.
        #[arg(long, default_value = "8081")]
        exporter_port: NonZeroU16,

        /// The name of the indexer docker image to use.
        #[cfg(feature = "kubernetes")]
        #[arg(long, default_value = "linera-indexer:latest")]
        indexer_image_name: String,

        /// The name of the explorer docker image to use.
        #[cfg(feature = "kubernetes")]
        #[arg(long, default_value = "linera-explorer:latest")]
        explorer_image_name: String,

        /// Use dual store (rocksdb and scylladb) instead of just scylladb. This is exclusive for
        /// kubernetes deployments.
        #[cfg(feature = "kubernetes")]
        #[arg(long, default_value = "false")]
        dual_store: bool,
    },

    /// Print a bash helper script to make `linera net up` easier to use. The script is
    /// meant to be installed in `~/.bash_profile` or sourced when needed.
    Helper,
}

#[derive(Clone, clap::Subcommand)]
pub enum WalletCommand {
    /// Show the contents of the wallet.
    Show {
        /// The chain to show the metadata.
        chain_id: Option<ChainId>,
        /// Only print a non-formatted list of the wallet's chain IDs.
        #[arg(long)]
        short: bool,
        /// Print only the chains that we have a key pair for.
        #[arg(long)]
        owned: bool,
    },

    /// Change the wallet default chain.
    SetDefault { chain_id: ChainId },

    /// Initialize a wallet from the genesis configuration.
    Init {
        /// The path to the genesis configuration for a Linera deployment. Either this or `--faucet`
        /// must be specified.
        ///
        /// Overrides `--faucet` if provided.
        #[arg(long = "genesis")]
        genesis_config_path: Option<PathBuf>,

        /// The address of a faucet.
        #[arg(long, env = "LINERA_FAUCET_URL")]
        faucet: Option<String>,

        /// Force this wallet to generate keys using a PRNG and a given seed. USE FOR
        /// TESTING ONLY.
        #[arg(long)]
        testing_prng_seed: Option<u64>,
    },

    /// Request a new chain from a faucet and add it to the wallet.
    RequestChain {
        /// The address of a faucet.
        #[arg(long, env = "LINERA_FAUCET_URL")]
        faucet: String,

        /// Whether this chain should become the default chain.
        #[arg(long)]
        set_default: bool,
    },

    /// Export the genesis configuration to a JSON file.
    ///
    /// By default, exports the genesis config from the current wallet. Alternatively,
    /// use `--faucet` to retrieve the genesis config directly from a faucet URL.
    ExportGenesis {
        /// Path to save the genesis configuration JSON file.
        output: PathBuf,

        /// The address of a faucet to retrieve the genesis config from.
        /// If not specified, the genesis config is read from the current wallet.
        #[arg(long)]
        faucet: Option<String>,
    },

    /// Add a new followed chain (i.e. a chain without keypair) to the wallet.
    FollowChain {
        /// The chain ID.
        chain_id: ChainId,
        /// Synchronize the new chain and download all its blocks from the validators.
        #[arg(long)]
        sync: bool,
    },

    /// Forgets the specified chain's keys. The chain will still be followed by the
    /// wallet.
    ForgetKeys { chain_id: ChainId },

    /// Forgets the specified chain, including the associated key pair.
    ForgetChain { chain_id: ChainId },
}

#[derive(Clone, clap::Subcommand)]
pub enum ChainCommand {
    /// Show the contents of a block.
    ShowBlock {
        /// The height of the block.
        height: BlockHeight,
        /// The chain to show the block (if not specified, the default chain from the
        /// wallet is used).
        chain_id: Option<ChainId>,
    },

    /// Show the chain description of a chain.
    ShowChainDescription {
        /// The chain ID to show (if not specified, the default chain from the wallet is
        /// used).
        chain_id: Option<ChainId>,
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
        /// This is used to locate the generated bytecode files. The generated bytecode files should
        /// be of the form `<name>_{contract,service}.wasm`.
        ///
        /// Defaults to the package name in Cargo.toml, with dashes replaced by
        /// underscores.
        name: Option<String>,

        /// An optional chain ID to publish the module. The default chain of the wallet
        /// is used otherwise.
        publisher: Option<ChainId>,

        /// The virtual machine runtime to use.
        #[arg(long, default_value = "wasm")]
        vm_runtime: VmRuntime,

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
        required_application_ids: Option<Vec<ApplicationId>>,
    },
}
