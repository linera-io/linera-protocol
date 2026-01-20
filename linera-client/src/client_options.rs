// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{BTreeMap, HashSet},
    fmt,
};

use linera_base::{
    data_types::{ApplicationPermissions, BlanketMessagePolicy, MessagePolicy, TimeDelta},
    identifiers::{AccountOwner, ApplicationId, ChainId, GenericApplicationId},
    ownership::ChainOwnership,
    time::Duration,
};
use linera_core::{
    client::{
        chain_client, DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
        DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
    },
    node::CrossChainMessageDelivery,
    DEFAULT_QUORUM_GRACE_PERIOD,
};
use linera_execution::ResourceControlPolicy;

#[cfg(not(web))]
use crate::client_metrics::TimingConfig;
use crate::util;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("there are {public_keys} public keys but {weights} weights")]
    MisalignedWeights { public_keys: usize, weights: usize },
    #[error("config error: {0}")]
    Config(#[from] crate::config::Error),
}

util::impl_from_infallible!(Error);

#[derive(Clone, clap::Parser, serde::Deserialize, tsify::Tsify)]
#[tsify(from_wasm_abi)]
#[group(skip)]
#[serde(default, rename_all = "camelCase")]
pub struct Options {
    /// Timeout for sending queries (milliseconds)
    #[arg(long = "send-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub send_timeout: Duration,

    /// Timeout for receiving responses (milliseconds)
    #[arg(long = "recv-timeout-ms", default_value = "4000", value_parser = util::parse_millis)]
    pub recv_timeout: Duration,

    /// The maximum number of incoming message bundles to include in a block proposal.
    #[arg(long, default_value = "10")]
    pub max_pending_message_bundles: usize,

    /// The duration in milliseconds after which an idle chain worker will free its memory.
    #[arg(
        long = "chain-worker-ttl-ms",
        default_value = "30000",
        env = "LINERA_CHAIN_WORKER_TTL_MS",
        value_parser = util::parse_millis,
    )]
    pub chain_worker_ttl: Duration,

    /// The duration, in milliseconds, after which an idle sender chain worker will
    /// free its memory.
    #[arg(
        long = "sender-chain-worker-ttl-ms",
        default_value = "1000",
        env = "LINERA_SENDER_CHAIN_WORKER_TTL_MS",
        value_parser = util::parse_millis
    )]
    pub sender_chain_worker_ttl: Duration,

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

    /// Whether to allow creating blocks in the fast round. Fast blocks have lower latency but
    /// must be used carefully so that there are never any conflicting fast block proposals.
    #[arg(long)]
    pub allow_fast_blocks: bool,

    /// (EXPERIMENTAL) Whether application services can persist in some cases between queries.
    #[arg(long)]
    pub long_lived_services: bool,

    /// The policy for handling incoming messages.
    #[arg(long, default_value_t, value_enum)]
    pub blanket_message_policy: BlanketMessagePolicy,

    /// A set of chains to restrict incoming messages from. By default, messages
    /// from all chains are accepted. To reject messages from all chains, specify
    /// an empty string.
    #[arg(long, value_parser = util::parse_chain_set)]
    pub restrict_chain_ids_to: Option<HashSet<ChainId>>,

    /// A set of application IDs. If specified, only bundles with at least one message from one of
    /// these applications will be accepted.
    #[arg(long, value_parser = util::parse_app_set)]
    pub reject_message_bundles_without_application_ids: Option<HashSet<GenericApplicationId>>,

    /// A set of application IDs. If specified, only bundles where all messages are from one of
    /// these applications will be accepted.
    #[arg(long, value_parser = util::parse_app_set)]
    pub reject_message_bundles_with_other_application_ids: Option<HashSet<GenericApplicationId>>,

    /// Enable timing reports during operations
    #[cfg(not(web))]
    #[arg(long)]
    pub timings: bool,

    /// Interval in seconds between timing reports (defaults to 5)
    #[cfg(not(web))]
    #[arg(long, default_value = "5")]
    pub timing_interval: u64,

    /// An additional delay, after reaching a quorum, to wait for additional validator signatures,
    /// as a fraction of time taken to reach quorum.
    #[arg(long, default_value_t = DEFAULT_QUORUM_GRACE_PERIOD)]
    pub quorum_grace_period: f64,

    /// The delay when downloading a blob, after which we try a second validator, in milliseconds.
    #[arg(
        long = "blob-download-timeout-ms",
        default_value = "1000",
        value_parser = util::parse_millis,
    )]
    pub blob_download_timeout: Duration,

    /// The delay when downloading a batch of certificates, after which we try a second validator,
    /// in milliseconds.
    #[arg(
        long = "cert-batch-download-timeout-ms",
        default_value = "1000",
        value_parser = util::parse_millis
    )]
    pub certificate_batch_download_timeout: Duration,

    /// Maximum number of certificates that we download at a time from one validator when
    /// synchronizing one of our chains.
    #[arg(
        long,
        default_value_t = DEFAULT_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
    )]
    pub certificate_download_batch_size: u64,

    /// Maximum number of sender certificates we try to download and receive in one go
    /// when syncing sender chains.
    #[arg(
        long,
        default_value_t = DEFAULT_SENDER_CERTIFICATE_DOWNLOAD_BATCH_SIZE,
    )]
    pub sender_certificate_download_batch_size: usize,

    /// Maximum number of tasks that can are joined concurrently in the client.
    #[arg(long, default_value = "100")]
    pub max_joined_tasks: usize,

    /// Maximum expected latency in milliseconds for score normalization.
    #[arg(
        long,
        default_value_t = linera_core::client::requests_scheduler::MAX_ACCEPTED_LATENCY_MS,
        env = "LINERA_REQUESTS_SCHEDULER_MAX_ACCEPTED_LATENCY_MS"
    )]
    pub max_accepted_latency_ms: f64,

    /// Time-to-live for cached responses in milliseconds.
    #[arg(
        long,
        default_value_t = linera_core::client::requests_scheduler::CACHE_TTL_MS,
        env = "LINERA_REQUESTS_SCHEDULER_CACHE_TTL_MS"
    )]
    pub cache_ttl_ms: u64,

    /// Maximum number of entries in the cache.
    #[arg(
        long,
        default_value_t = linera_core::client::requests_scheduler::CACHE_MAX_SIZE,
        env = "LINERA_REQUESTS_SCHEDULER_CACHE_MAX_SIZE"
    )]
    pub cache_max_size: usize,

    /// Maximum latency for an in-flight request before we stop deduplicating it (in milliseconds).
    #[arg(
        long,
        default_value_t = linera_core::client::requests_scheduler::MAX_REQUEST_TTL_MS,
        env = "LINERA_REQUESTS_SCHEDULER_MAX_REQUEST_TTL_MS"
    )]
    pub max_request_ttl_ms: u64,

    /// Smoothing factor for Exponential Moving Averages (0 < alpha < 1).
    /// Higher values give more weight to recent observations.
    /// Typical values are between 0.01 and 0.5.
    /// A value of 0.1 means that 10% of the new observation is considered
    /// and 90% of the previous average is retained.
    #[arg(
        long,
        default_value_t = linera_core::client::requests_scheduler::ALPHA_SMOOTHING_FACTOR,
        env = "LINERA_REQUESTS_SCHEDULER_ALPHA"
    )]
    pub alpha: f64,

    /// Delay in milliseconds between starting requests to different peers.
    /// This helps to stagger requests and avoid overwhelming the network.
    #[arg(
        long,
        default_value_t = linera_core::client::requests_scheduler::STAGGERED_DELAY_MS,
        env = "LINERA_REQUESTS_SCHEDULER_ALTERNATIVE_PEERS_RETRY_DELAY_MS"
    )]
    pub alternative_peers_retry_delay_ms: u64,

    #[serde(flatten)]
    #[clap(flatten)]
    pub chain_listener_config: crate::chain_listener::ChainListenerConfig,
}

impl Default for Options {
    fn default() -> Self {
        use clap::Parser;

        #[derive(Parser)]
        struct OptionsParser {
            #[clap(flatten)]
            options: Options,
        }

        OptionsParser::try_parse_from(std::iter::empty::<std::ffi::OsString>())
            .expect("Options has no required arguments")
            .options
    }
}

impl Options {
    /// Creates [`chain_client::Options`] with the corresponding values.
    pub(crate) fn to_chain_client_options(&self) -> chain_client::Options {
        let message_policy = MessagePolicy::new(
            self.blanket_message_policy,
            self.restrict_chain_ids_to.clone(),
            self.reject_message_bundles_without_application_ids.clone(),
            self.reject_message_bundles_with_other_application_ids
                .clone(),
        );
        let cross_chain_message_delivery =
            CrossChainMessageDelivery::new(self.wait_for_outgoing_messages);
        chain_client::Options {
            max_pending_message_bundles: self.max_pending_message_bundles,
            message_policy,
            cross_chain_message_delivery,
            quorum_grace_period: self.quorum_grace_period,
            blob_download_timeout: self.blob_download_timeout,
            certificate_batch_download_timeout: self.certificate_batch_download_timeout,
            certificate_download_batch_size: self.certificate_download_batch_size,
            sender_certificate_download_batch_size: self.sender_certificate_download_batch_size,
            max_joined_tasks: self.max_joined_tasks,
            allow_fast_blocks: self.allow_fast_blocks,
        }
    }

    /// Creates [`TimingConfig`] with the corresponding values.
    #[cfg(not(web))]
    pub(crate) fn to_timing_config(&self) -> TimingConfig {
        TimingConfig {
            enabled: self.timings,
            report_interval_secs: self.timing_interval,
        }
    }

    /// Creates [`RequestsSchedulerConfig`] with the corresponding values.
    pub(crate) fn to_requests_scheduler_config(
        &self,
    ) -> linera_core::client::RequestsSchedulerConfig {
        linera_core::client::RequestsSchedulerConfig {
            max_accepted_latency_ms: self.max_accepted_latency_ms,
            cache_ttl_ms: self.cache_ttl_ms,
            cache_max_size: self.cache_max_size,
            max_request_ttl_ms: self.max_request_ttl_ms,
            alpha: self.alpha,
            retry_delay_ms: self.alternative_peers_retry_delay_ms,
        }
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct ChainOwnershipConfig {
    /// A JSON list of the new super owners. Absence of the option leaves the current
    /// set of super owners unchanged.
    // NOTE (applies to all fields): we need the std::option:: and std::vec:: qualifiers in order
    // to throw off the #[derive(Args)] macro's automatic inference of the type it should expect
    // from the parser. Without it, it infers the inner type (so either ApplicationId or
    // Vec<ApplicationId>), which is not what we want here - we want the parsers to return the full
    // expected types.
    #[arg(long, value_parser = util::parse_json::<Vec<AccountOwner>>)]
    pub super_owners: Option<std::vec::Vec<AccountOwner>>,

    /// A JSON map of the new owners to their weights. Absence of the option leaves the current
    /// set of owners unchanged.
    #[arg(long, value_parser = util::parse_json::<BTreeMap<AccountOwner, u64>>)]
    pub owners: Option<BTreeMap<AccountOwner, u64>>,

    /// The leader of the first single-leader round. If set to null, this is random like other
    /// rounds. Absence of the option leaves the current setting unchanged.
    #[arg(long, value_parser = util::parse_json::<Option<AccountOwner>>)]
    pub first_leader: Option<std::option::Option<AccountOwner>>,

    /// The number of rounds in which every owner can propose blocks, i.e. the first round
    /// number in which only a single designated leader is allowed to propose blocks. "null" is
    /// equivalent to 2^32 - 1. Absence of the option leaves the current setting unchanged.
    #[arg(long, value_parser = util::parse_json::<Option<u32>>)]
    pub multi_leader_rounds: Option<std::option::Option<u32>>,

    /// Whether the multi-leader rounds are unrestricted, i.e. not limited to chain owners.
    /// This should only be `true` on chains with restrictive application permissions and an
    /// application-based mechanism to select block proposers.
    #[arg(long)]
    pub open_multi_leader_rounds: bool,

    /// The duration of the fast round, in milliseconds. "null" means the fast round will
    /// not time out. Absence of the option leaves the current setting unchanged.
    #[arg(long = "fast-round-ms", value_parser = util::parse_json_optional_millis_delta)]
    pub fast_round_duration: Option<std::option::Option<TimeDelta>>,

    /// The duration of the first single-leader and all multi-leader rounds. Absence of
    /// the option leaves the current setting unchanged.
    #[arg(
        long = "base-timeout-ms",
        value_parser = util::parse_millis_delta
    )]
    pub base_timeout: Option<TimeDelta>,

    /// The number of milliseconds by which the timeout increases after each
    /// single-leader round. Absence of the option leaves the current setting unchanged.
    #[arg(
        long = "timeout-increment-ms",
        value_parser = util::parse_millis_delta
    )]
    pub timeout_increment: Option<TimeDelta>,

    /// The age of an incoming tracked or protected message after which the validators start
    /// transitioning the chain to fallback mode, in milliseconds. Absence of the option
    /// leaves the current setting unchanged.
    #[arg(
        long = "fallback-duration-ms",
        value_parser = util::parse_millis_delta
    )]
    pub fallback_duration: Option<TimeDelta>,
}

impl ChainOwnershipConfig {
    pub fn update(self, chain_ownership: &mut ChainOwnership) -> Result<(), Error> {
        let ChainOwnershipConfig {
            super_owners,
            owners,
            first_leader,
            multi_leader_rounds,
            fast_round_duration,
            open_multi_leader_rounds,
            base_timeout,
            timeout_increment,
            fallback_duration,
        } = self;

        if let Some(owners) = owners {
            chain_ownership.owners = owners;
        }

        if let Some(super_owners) = super_owners {
            chain_ownership.super_owners = super_owners.into_iter().collect();
        }

        if let Some(first_leader) = first_leader {
            chain_ownership.first_leader = first_leader;
        }
        if let Some(multi_leader_rounds) = multi_leader_rounds {
            chain_ownership.multi_leader_rounds = multi_leader_rounds.unwrap_or(u32::MAX);
        }

        chain_ownership.open_multi_leader_rounds = open_multi_leader_rounds;

        if let Some(fast_round_duration) = fast_round_duration {
            chain_ownership.timeout_config.fast_round_duration = fast_round_duration;
        }
        if let Some(base_timeout) = base_timeout {
            chain_ownership.timeout_config.base_timeout = base_timeout;
        }
        if let Some(timeout_increment) = timeout_increment {
            chain_ownership.timeout_config.timeout_increment = timeout_increment;
        }
        if let Some(fallback_duration) = fallback_duration {
            chain_ownership.timeout_config.fallback_duration = fallback_duration;
        }

        Ok(())
    }
}

impl TryFrom<ChainOwnershipConfig> for ChainOwnership {
    type Error = Error;

    fn try_from(config: ChainOwnershipConfig) -> Result<ChainOwnership, Error> {
        let mut chain_ownership = ChainOwnership::default();
        config.update(&mut chain_ownership)?;
        Ok(chain_ownership)
    }
}

#[derive(Debug, Clone, clap::Args)]
pub struct ApplicationPermissionsConfig {
    /// A JSON list of applications allowed to execute operations on this chain. If set to null, all
    /// operations will be allowed. Otherwise, only operations from the specified applications are
    /// allowed, and no system operations. Absence of the option leaves current permissions
    /// unchanged.
    // NOTE (applies to all fields): we need the std::option:: and std::vec:: qualifiers in order
    // to throw off the #[derive(Args)] macro's automatic inference of the type it should expect
    // from the parser. Without it, it infers the inner type (so either ApplicationId or
    // Vec<ApplicationId>), which is not what we want here - we want the parsers to return the full
    // expected types.
    #[arg(long, value_parser = util::parse_json::<Option<Vec<ApplicationId>>>)]
    pub execute_operations: Option<std::option::Option<Vec<ApplicationId>>>,
    /// A JSON list of applications, such that at least one operation or incoming message from each
    /// of these applications must occur in every block. Absence of the option leaves
    /// current mandatory applications unchanged.
    #[arg(long, value_parser = util::parse_json::<Vec<ApplicationId>>)]
    pub mandatory_applications: Option<std::vec::Vec<ApplicationId>>,
    /// A JSON list of applications allowed to manage the chain: close it, change application
    /// permissions, and change ownership. Absence of the option leaves current managing
    /// applications unchanged.
    #[arg(long, value_parser = util::parse_json::<Vec<ApplicationId>>)]
    pub manage_chain: Option<std::vec::Vec<ApplicationId>>,
    /// A JSON list of applications that are allowed to call services as oracles on the current
    /// chain using the system API. If set to null, all applications will be able to do
    /// so. Absence of the option leaves the current value of the setting unchanged.
    #[arg(long, value_parser = util::parse_json::<Option<Vec<ApplicationId>>>)]
    pub call_service_as_oracle: Option<std::option::Option<Vec<ApplicationId>>>,
    /// A JSON list of applications that are allowed to make HTTP requests on the current chain
    /// using the system API. If set to null, all applications will be able to do so.
    /// Absence of the option leaves the current value of the setting unchanged.
    #[arg(long, value_parser = util::parse_json::<Option<Vec<ApplicationId>>>)]
    pub make_http_requests: Option<std::option::Option<Vec<ApplicationId>>>,
}

impl ApplicationPermissionsConfig {
    pub fn update(self, application_permissions: &mut ApplicationPermissions) {
        if let Some(execute_operations) = self.execute_operations {
            application_permissions.execute_operations = execute_operations;
        }
        if let Some(mandatory_applications) = self.mandatory_applications {
            application_permissions.mandatory_applications = mandatory_applications;
        }
        if let Some(manage_chain) = self.manage_chain {
            application_permissions.manage_chain = manage_chain;
        }
        if let Some(call_service_as_oracle) = self.call_service_as_oracle {
            application_permissions.call_service_as_oracle = call_service_as_oracle;
        }
        if let Some(make_http_requests) = self.make_http_requests {
            application_permissions.make_http_requests = make_http_requests;
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
