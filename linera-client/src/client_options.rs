// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, fmt, iter, num::NonZeroUsize, path::PathBuf};

use linera_base::{
    data_types::{ApplicationPermissions, TimeDelta},
    identifiers::{AccountOwner, ApplicationId, ChainId},
    ownership::{ChainOwnership, TimeoutConfig},
    time::Duration,
};
use linera_core::{client::BlanketMessagePolicy, DEFAULT_GRACE_PERIOD};
use linera_execution::{ResourceControlPolicy, WasmRuntime};

#[cfg(any(with_indexed_db, not(with_persist)))]
use crate::{config::WalletState, wallet::Wallet};
use crate::{persistent, util};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("no wallet found")]
    NonexistentWallet,
    #[error("there are {public_keys} public keys but {weights} weights")]
    MisalignedWeights { public_keys: usize, weights: usize },
    #[error("persistence error: {0}")]
    Persistence(#[from] Box<dyn std::error::Error + Send + Sync>),
    #[error("config error: {0}")]
    Config(#[from] crate::config::Error),
}

#[cfg(feature = "fs")]
util::impl_from_dynamic!(Error:Persistence, persistent::file::Error);

#[cfg(with_indexed_db)]
util::impl_from_dynamic!(Error:Persistence, persistent::indexed_db::Error);

util::impl_from_infallible!(Error);

#[derive(Clone, clap::Parser)]
pub struct ClientContextOptions {
    /// Sets the file storing the private state of user chains (an empty one will be created if missing)
    #[arg(long = "wallet")]
    pub wallet_state_path: Option<PathBuf>,

    /// Given an ASCII alphanumeric parameter `X`, read the wallet state and the wallet
    /// storage config from the environment variables `LINERA_WALLET_{X}` and
    /// `LINERA_STORAGE_{X}` instead of `LINERA_WALLET` and
    /// `LINERA_STORAGE`.
    #[arg(long, short = 'w', value_parser = util::parse_ascii_alphanumeric_string)]
    pub with_wallet: Option<String>,

    /// Timeout for sending queries (milliseconds)
    #[arg(long = "send-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub send_timeout: Duration,

    /// Timeout for receiving responses (milliseconds)
    #[arg(long = "recv-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub recv_timeout: Duration,

    /// The maximum number of incoming message bundles to include in a block proposal.
    #[arg(long, default_value = "10")]
    pub max_pending_message_bundles: usize,

    /// The WebAssembly runtime to use.
    #[arg(long)]
    pub wasm_runtime: Option<WasmRuntime>,

    /// The maximal number of chains loaded in memory at a given time.
    #[arg(long, default_value = "40")]
    pub max_loaded_chains: NonZeroUsize,

    /// The maximal number of simultaneous queries to the database
    #[arg(long)]
    pub max_concurrent_queries: Option<usize>,

    /// The maximal number of simultaneous stream queries to the database
    #[arg(long, default_value = "10")]
    pub max_stream_queries: usize,

    /// The maximal memory used in the storage cache.
    #[arg(long, default_value = "10000000")]
    pub max_cache_size: usize,

    /// The maximal size of an entry in the storage cache.
    #[arg(long, default_value = "1000000")]
    pub max_entry_size: usize,

    /// The maximal number of entries in the storage cache.
    #[arg(long, default_value = "1000")]
    pub max_cache_entries: usize,

    /// Delay increment for retrying to connect to a validator.
    #[arg(
        long = "retry-delay-ms",
        default_value = "1000",
        value_parser = util::parse_millis
    )]
    pub retry_delay: Duration,

    /// Number of times to retry connecting to a validator.
    #[arg(long, default_value = "10")]
    pub max_retries: u32,

    /// Whether to wait until a quorum of validators has confirmed that all sent cross-chain
    /// messages have been delivered.
    #[arg(long)]
    pub wait_for_outgoing_messages: bool,

    /// (EXPERIMENTAL) Whether application services can persist in some cases between queries.
    #[arg(long)]
    pub long_lived_services: bool,

    /// The number of Tokio worker threads to use.
    #[arg(long, env = "LINERA_CLIENT_TOKIO_THREADS")]
    pub tokio_threads: Option<usize>,

    /// The policy for handling incoming messages.
    #[arg(long, default_value = "accept")]
    pub blanket_message_policy: BlanketMessagePolicy,

    /// A set of chains to restrict incoming messages from. By default, messages
    /// from all chains are accepted. To reject messages from all chains, specify
    /// an empty string.
    #[arg(long, value_parser = util::parse_chain_set)]
    pub restrict_chain_ids_to: Option<HashSet<ChainId>>,

    /// An additional delay, after reaching a quorum, to wait for additional validator signatures,
    /// as a fraction of time taken to reach quorum.
    #[arg(long, default_value_t = DEFAULT_GRACE_PERIOD)]
    pub grace_period: f64,

    /// The delay when downloading a blob, after which we try a second validator, in milliseconds.
    #[arg(
        long = "blob-download-timeout-ms",
        default_value = "1000",
        value_parser = util::parse_millis
    )]
    pub blob_download_timeout: Duration,
}

#[cfg(with_indexed_db)]
impl ClientContextOptions {
    pub async fn wallet(&self) -> Result<WalletState<persistent::IndexedDb<Wallet>>, Error> {
        Ok(WalletState::new(
            persistent::IndexedDb::read("linera-wallet")
                .await?
                .ok_or(Error::NonexistentWallet)?,
        ))
    }
}

#[cfg(not(with_persist))]
impl ClientContextOptions {
    pub async fn wallet(&self) -> Result<WalletState<persistent::Memory<Wallet>>, Error> {
        #![allow(unreachable_code)]
        let _wallet = unimplemented!("No persistence backend selected for wallet; please use one of the `fs` or `indexed-db` features");
        Ok(WalletState::new(persistent::Memory::new(_wallet)))
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct ChainOwnershipConfig {
    /// The new super owners.
    #[arg(long, num_args(0..))]
    super_owners: Vec<AccountOwner>,

    /// The new regular owners.
    #[arg(long, num_args(0..))]
    owners: Vec<AccountOwner>,

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

    /// Whether the multi-leader rounds are unrestricted, i.e. not limited to chain owners.
    /// This should only be `true` on chains with restrictive application permissions and an
    /// application-based mechanism to select block proposers.
    #[arg(long)]
    open_multi_leader_rounds: bool,

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
            super_owners,
            owners,
            owner_weights,
            multi_leader_rounds,
            fast_round_duration,
            open_multi_leader_rounds,
            base_timeout,
            timeout_increment,
            fallback_duration,
        } = config;
        if !owner_weights.is_empty() && owner_weights.len() != owners.len() {
            return Err(Error::MisalignedWeights {
                public_keys: owners.len(),
                weights: owner_weights.len(),
            });
        }
        let super_owners = super_owners.into_iter().collect();
        let owners = owners
            .into_iter()
            .zip(owner_weights.into_iter().chain(iter::repeat(100)))
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
            open_multi_leader_rounds,
            timeout_config,
        })
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct ApplicationPermissionsConfig {
    /// If present, only operations from the specified applications are allowed, and
    /// no system operations. Otherwise all operations are allowed.
    #[arg(long)]
    pub execute_operations: Option<Vec<ApplicationId>>,
    /// At least one operation or incoming message from each of these applications must occur in
    /// every block.
    #[arg(long)]
    pub mandatory_applications: Option<Vec<ApplicationId>>,
    /// These applications are allowed to close the current chain using the system API.
    #[arg(long)]
    pub close_chain: Option<Vec<ApplicationId>>,
    /// These applications are allowed to change the application permissions on the current chain
    /// using the system API.
    #[arg(long)]
    pub change_application_permissions: Option<Vec<ApplicationId>>,
    /// These applications are allowed to call services as oracles on the current chain using the
    /// system API.
    #[arg(long)]
    pub call_service_as_oracle: Option<Vec<ApplicationId>>,
    /// These applications are allowed to make HTTP requests on the current chain using the system
    /// API.
    #[arg(long)]
    pub make_http_requests: Option<Vec<ApplicationId>>,
}

impl From<ApplicationPermissionsConfig> for ApplicationPermissions {
    fn from(config: ApplicationPermissionsConfig) -> ApplicationPermissions {
        ApplicationPermissions {
            execute_operations: config.execute_operations,
            mandatory_applications: config.mandatory_applications.unwrap_or_default(),
            close_chain: config.close_chain.unwrap_or_default(),
            change_application_permissions: config
                .change_application_permissions
                .unwrap_or_default(),
            call_service_as_oracle: config.call_service_as_oracle,
            make_http_requests: config.make_http_requests,
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResourceControlPolicyConfig {
    NoFees,
    Testnet,
    #[cfg(with_testing)]
    OnlyFuel,
    #[cfg(with_testing)]
    FuelAndBlock,
    #[cfg(with_testing)]
    AllCategories,
}

impl ResourceControlPolicyConfig {
    pub fn into_policy(self) -> ResourceControlPolicy {
        match self {
            ResourceControlPolicyConfig::NoFees => ResourceControlPolicy::no_fees(),
            ResourceControlPolicyConfig::Testnet => ResourceControlPolicy::testnet(),
            #[cfg(with_testing)]
            ResourceControlPolicyConfig::OnlyFuel => ResourceControlPolicy::only_fuel(),
            #[cfg(with_testing)]
            ResourceControlPolicyConfig::FuelAndBlock => ResourceControlPolicy::fuel_and_block(),
            #[cfg(with_testing)]
            ResourceControlPolicyConfig::AllCategories => ResourceControlPolicy::all_categories(),
        }
    }
}

impl std::str::FromStr for ResourceControlPolicyConfig {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        clap::ValueEnum::from_str(s, true)
    }
}

impl fmt::Display for ResourceControlPolicyConfig {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}
