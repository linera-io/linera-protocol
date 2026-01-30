// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use linera_base::{
    data_types::Amount,
    identifiers::{Account, AccountOwner, ApplicationId, ChainId},
    time::Instant,
};
use linera_core::{
    client::{ChainClient, ChainClientError},
    Environment,
};
use linera_execution::{system::SystemOperation, Operation};
use linera_sdk::abis::fungible::{self, FungibleOperation};
use num_format::{Locale, ToFormattedString};
use prometheus_parse::{HistogramCount, Scrape, Value};
use rand::{rngs::SmallRng, seq::SliceRandom, thread_rng, Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, Barrier, Notify},
    task, time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument as _};

use crate::chain_listener::{ChainListener, ClientContext};

/// The mode of operation for the benchmark.
#[derive(Clone, Debug)]
pub enum BenchmarkMode {
    /// Benchmark native token transfers between chains.
    NativeTransfer,
    /// Benchmark fungible token transfers between chains.
    FungibleTransfer { application_id: ApplicationId },
    /// Benchmark PM (Prediction Market) order submission.
    Pm {
        pm_engine_id: ApplicationId,
        market_chain_id: ChainId,
        pattern: OrderPattern,
        price_scale: u64,
    },
}

impl BenchmarkMode {
    /// Create a BenchmarkMode from the legacy fungible_application_id parameter.
    pub fn from_fungible_application_id(fungible_application_id: Option<ApplicationId>) -> Self {
        match fungible_application_id {
            Some(application_id) => BenchmarkMode::FungibleTransfer { application_id },
            None => BenchmarkMode::NativeTransfer,
        }
    }

    /// Returns the market chain ID if this is a PM benchmark, None otherwise.
    pub fn market_chain_id(&self) -> Option<ChainId> {
        match self {
            BenchmarkMode::Pm {
                market_chain_id, ..
            } => Some(*market_chain_id),
            _ => None,
        }
    }
}

/// Pattern for generating PM orders (for CLI parsing).
///
/// These types mirror pm-engine but are defined here to avoid a dependency on pm-app.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum OrderPatternArg {
    /// All orders will match. Generates YES bid + NO bid pairs at complementary prices.
    #[default]
    Matching,
    /// Mix of matching and non-matching orders. Use --match-probability to configure
    /// what fraction of orders will match (default: 0.5).
    Mixed,
}

/// Pattern for generating PM orders (internal representation with probability).
#[derive(Clone, Copy, Debug)]
pub enum OrderPattern {
    /// All orders will match.
    Matching,
    /// Mix of matching and non-matching orders with configurable probability.
    Mixed { match_probability: f64 },
}

impl OrderPattern {
    /// Create from CLI args.
    ///
    /// Returns an error if `--match-probability` is set but the pattern is not `Mixed`.
    pub fn from_args(
        pattern: OrderPatternArg,
        match_probability: Option<f64>,
    ) -> anyhow::Result<Self> {
        match pattern {
            OrderPatternArg::Matching => {
                if match_probability.is_some() {
                    anyhow::bail!(
                        "--match-probability can only be used with --order-pattern mixed"
                    );
                }
                Ok(OrderPattern::Matching)
            }
            OrderPatternArg::Mixed => Ok(OrderPattern::Mixed {
                match_probability: match_probability.unwrap_or(0.5),
            }),
        }
    }
}

/// PM order nature (which side of the market).
/// IMPORTANT: Variant order must match pm-engine's OrderNature for BCS serialization.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum PmOrderNature {
    /// A bid for buying YES tokens (index 0)
    YesBid,
    /// An ask for selling YES tokens (index 1) - not used in benchmark
    YesAsk,
    /// A bid for buying NO tokens (index 2)
    NoBid,
    /// An ask for selling NO tokens (index 3) - not used in benchmark
    NoAsk,
}

/// PM order validity.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum PmValidity {
    /// Order remains until explicitly cancelled.
    UntilCanceled,
    /// Order must be filled immediately or cancelled.
    Immediate,
}

/// A PM order to insert into the order book.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PmOrder {
    Insert {
        quantity: Amount,
        nature: PmOrderNature,
        price: u64,
        validity: PmValidity,
    },
}

/// PM engine operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum PmEngineOperation {
    ExecuteOrder { order: PmOrder, owner: AccountOwner },
}

/// Generates PM orders based on a pattern.
///
/// For matching orders to work in a prediction market:
/// - YES bid at price P + NO bid at price (100-P) = prices sum to 100 = MATCH
/// - When matched, the engine mints 1 YES + 1 NO token backed by their combined payment
///
/// This generator maintains state to produce complementary pairs:
/// - First call: YES bid at price P
/// - Second call: NO bid at price (100-P)
/// - Repeat...
pub struct PmOrderGenerator {
    pattern: OrderPattern,
    price_scale: u64,
    /// State for matching pairs: stores the YES bid price when we need to generate the NO bid
    pending_yes_price: Option<u64>,
}

impl PmOrderGenerator {
    pub fn new(pattern: OrderPattern, price_scale: u64) -> Self {
        Self {
            pattern,
            price_scale,
            pending_yes_price: None,
        }
    }

    /// Generate the next order based on the pattern.
    pub fn next_order(&mut self, rng: &mut impl Rng, owner: AccountOwner) -> PmEngineOperation {
        let order = match self.pattern {
            OrderPattern::Matching => self.matching_order(rng),
            OrderPattern::Mixed { match_probability } => {
                // If we have a pending YES price, we must complete the pair.
                // Otherwise, flip the coin to decide whether to start a new matching pair.
                if self.pending_yes_price.is_some() || rng.gen_bool(match_probability) {
                    self.matching_order(rng)
                } else {
                    self.non_matching_order(rng)
                }
            }
        };
        PmEngineOperation::ExecuteOrder { order, owner }
    }

    /// Generate an order that won't match (bids at very low prices).
    /// These orders sit in the order book and never execute.
    fn non_matching_order(&self, rng: &mut impl Rng) -> PmOrder {
        // Very low bids that won't find matching counterparties
        let price = rng.gen_range(1..self.price_scale / 10); // 1-10%
        let nature = if rng.gen_bool(0.5) {
            PmOrderNature::YesBid
        } else {
            PmOrderNature::NoBid
        };

        PmOrder::Insert {
            quantity: Amount::from_millis(rng.gen_range(100..1000)),
            nature,
            price,
            validity: PmValidity::UntilCanceled,
        }
    }

    /// Generate a matching order pair (YES bid + NO bid at complementary prices).
    /// Called twice: first returns YES bid, second returns complementary NO bid.
    fn matching_order(&mut self, rng: &mut impl Rng) -> PmOrder {
        if let Some(yes_price) = self.pending_yes_price.take() {
            // Second order of the pair: NO bid at complementary price
            // YES price + NO price = price_scale (e.g., 60 + 40 = 100)
            let no_price = self.price_scale - yes_price;
            PmOrder::Insert {
                quantity: Amount::from_millis(rng.gen_range(100..500)),
                nature: PmOrderNature::NoBid,
                price: no_price,
                validity: PmValidity::UntilCanceled,
            }
        } else {
            // First order of the pair: YES bid
            // Price between 40-60% for realistic mid-range orders
            let yes_price = self.price_scale * 4 / 10 + rng.gen_range(0..self.price_scale * 2 / 10);
            self.pending_yes_price = Some(yes_price);
            PmOrder::Insert {
                quantity: Amount::from_millis(rng.gen_range(100..500)),
                nature: PmOrderNature::YesBid,
                price: yes_price,
                validity: PmValidity::UntilCanceled,
            }
        }
    }
}

/// Create a PM operation to send to the pm-engine.
pub fn create_pm_operation(pm_engine_id: ApplicationId, operation: PmEngineOperation) -> Operation {
    let bytes = bcs::to_bytes(&operation).expect("should serialize pm-engine operation");
    Operation::User {
        application_id: pm_engine_id,
        bytes,
    }
}

const PROXY_LATENCY_P99_THRESHOLD: f64 = 400.0;
const LATENCY_METRIC_PREFIX: &str = "linera_proxy_request_latency";

#[derive(Debug, thiserror::Error)]
pub enum BenchmarkError {
    #[error("Failed to join task: {0}")]
    JoinError(#[from] task::JoinError),
    #[error("Chain client error: {0}")]
    ChainClient(#[from] ChainClientError),
    #[error("Current histogram count is less than previous histogram count")]
    HistogramCountMismatch,
    #[error("Expected histogram value, got {0:?}")]
    ExpectedHistogramValue(Value),
    #[error("Expected untyped value, got {0:?}")]
    ExpectedUntypedValue(Value),
    #[error("Incomplete histogram data")]
    IncompleteHistogramData,
    #[error("Could not compute quantile")]
    CouldNotComputeQuantile,
    #[error("Bucket boundaries do not match: {0} vs {1}")]
    BucketBoundariesDoNotMatch(f64, f64),
    #[error("Reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("Io error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Previous histogram snapshot does not exist: {0}")]
    PreviousHistogramSnapshotDoesNotExist(String),
    #[error("No data available yet to calculate p99")]
    NoDataYetForP99Calculation,
    #[error("Unexpected empty bucket")]
    UnexpectedEmptyBucket,
    #[error("Failed to send unit message: {0}")]
    TokioSendUnitError(#[from] mpsc::error::SendError<()>),
    #[error("Config file not found: {0}")]
    ConfigFileNotFound(std::path::PathBuf),
    #[error("Failed to load config file: {0}")]
    ConfigLoadError(#[from] anyhow::Error),
    #[error("Could not find enough chains in wallet alone: needed {0}, but only found {1}")]
    NotEnoughChainsInWallet(usize, usize),
    #[error("Random number generator error: {0}")]
    RandError(#[from] rand::Error),
}

#[derive(Debug)]
struct HistogramSnapshot {
    buckets: Vec<HistogramCount>,
    count: f64,
    sum: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct BenchmarkConfig {
    pub chain_ids: Vec<ChainId>,
}

impl BenchmarkConfig {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

pub struct Benchmark<Env: Environment> {
    _phantom: std::marker::PhantomData<Env>,
}

impl<Env: Environment> Benchmark<Env> {
    #[expect(clippy::too_many_arguments)]
    pub async fn run_benchmark<C: ClientContext<Environment = Env> + 'static>(
        bps: usize,
        chain_clients: Vec<ChainClient<Env>>,
        all_chains: Vec<ChainId>,
        transactions_per_block: usize,
        mode: BenchmarkMode,
        health_check_endpoints: Option<String>,
        runtime_in_seconds: Option<u64>,
        delay_between_chains_ms: Option<u64>,
        chain_listener: ChainListener<C>,
        shutdown_notifier: &CancellationToken,
        single_destination_per_block: bool,
    ) -> Result<(), BenchmarkError> {
        let num_chains = chain_clients.len();
        let bps_counts = (0..num_chains)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let notifier = Arc::new(Notify::new());
        let barrier = Arc::new(Barrier::new(num_chains + 1));

        let chain_listener_result = chain_listener.run().await;

        let chain_listener_handle =
            tokio::spawn(async move { chain_listener_result?.await }.in_current_span());

        let bps_control_task = Self::bps_control_task(
            &barrier,
            shutdown_notifier,
            &bps_counts,
            &notifier,
            transactions_per_block,
            bps,
        );

        let (runtime_control_task, runtime_control_sender) =
            Self::runtime_control_task(shutdown_notifier, runtime_in_seconds, num_chains);

        let num_chains = chain_clients.len();
        let bps_initial_share = bps / num_chains;
        let mut bps_remainder = bps % num_chains;
        let mut join_set = task::JoinSet::<Result<(), BenchmarkError>>::new();
        for (chain_idx, chain_client) in chain_clients.into_iter().enumerate() {
            let shutdown_notifier_clone = shutdown_notifier.clone();
            let barrier_clone = barrier.clone();
            let bps_count_clone = bps_counts[chain_idx].clone();
            let notifier_clone = notifier.clone();
            let runtime_control_sender_clone = runtime_control_sender.clone();
            let all_chains_clone = all_chains.clone();
            let mode_clone = mode.clone();
            let bps_share = if bps_remainder > 0 {
                bps_remainder -= 1;
                bps_initial_share + 1
            } else {
                bps_initial_share
            };
            let chain_id = chain_client.chain_id();
            join_set.spawn(
                async move {
                    Box::pin(Self::run_benchmark_internal(
                        chain_idx,
                        chain_id,
                        bps_share,
                        chain_client,
                        all_chains_clone,
                        transactions_per_block,
                        mode_clone,
                        shutdown_notifier_clone,
                        bps_count_clone,
                        barrier_clone,
                        notifier_clone,
                        runtime_control_sender_clone,
                        delay_between_chains_ms,
                        single_destination_per_block,
                    ))
                    .await?;

                    Ok(())
                }
                .instrument(tracing::info_span!("chain_id", chain_id = ?chain_id)),
            );
        }

        let metrics_watcher =
            Self::metrics_watcher(health_check_endpoints, shutdown_notifier).await?;

        // Wait for tasks and fail immediately if any task returns an error or panics
        while let Some(result) = join_set.join_next().await {
            let inner_result = result?;
            if let Err(e) = inner_result {
                error!("Benchmark task failed: {}", e);
                shutdown_notifier.cancel();
                join_set.abort_all();
                return Err(e);
            }
        }
        info!("All benchmark tasks completed successfully");

        bps_control_task.await?;
        if let Some(metrics_watcher) = metrics_watcher {
            metrics_watcher.await??;
        }
        if let Some(runtime_control_task) = runtime_control_task {
            runtime_control_task.await?;
        }

        if let Err(e) = chain_listener_handle.await? {
            tracing::error!("chain listener error: {e}");
        }

        Ok(())
    }

    // The bps control task will control the BPS from the threads.
    fn bps_control_task(
        barrier: &Arc<Barrier>,
        shutdown_notifier: &CancellationToken,
        bps_counts: &[Arc<AtomicUsize>],
        notifier: &Arc<Notify>,
        transactions_per_block: usize,
        bps: usize,
    ) -> task::JoinHandle<()> {
        let shutdown_notifier = shutdown_notifier.clone();
        let bps_counts = bps_counts.to_vec();
        let notifier = notifier.clone();
        let barrier = barrier.clone();
        task::spawn(
            async move {
                barrier.wait().await;
                let mut one_second_interval = time::interval(time::Duration::from_secs(1));
                loop {
                    if shutdown_notifier.is_cancelled() {
                        info!("Shutdown signal received in bps control task");
                        break;
                    }
                    one_second_interval.tick().await;
                    let current_bps_count: usize = bps_counts
                        .iter()
                        .map(|count| count.swap(0, Ordering::Relaxed))
                        .sum();
                    notifier.notify_waiters();
                    let formatted_current_bps = current_bps_count.to_formatted_string(&Locale::en);
                    let formatted_current_tps = (current_bps_count * transactions_per_block)
                        .to_formatted_string(&Locale::en);
                    let formatted_tps_goal =
                        (bps * transactions_per_block).to_formatted_string(&Locale::en);
                    let formatted_bps_goal = bps.to_formatted_string(&Locale::en);
                    if current_bps_count >= bps {
                        info!(
                            "Achieved {} BPS/{} TPS",
                            formatted_current_bps, formatted_current_tps
                        );
                    } else {
                        warn!(
                            "Failed to achieve {} BPS/{} TPS, only achieved {} BPS/{} TPS",
                            formatted_bps_goal,
                            formatted_tps_goal,
                            formatted_current_bps,
                            formatted_current_tps,
                        );
                    }
                }

                info!("Exiting bps control task");
            }
            .instrument(tracing::info_span!("bps_control")),
        )
    }

    async fn metrics_watcher(
        health_check_endpoints: Option<String>,
        shutdown_notifier: &CancellationToken,
    ) -> Result<Option<task::JoinHandle<Result<(), BenchmarkError>>>, BenchmarkError> {
        if let Some(health_check_endpoints) = health_check_endpoints {
            let metrics_addresses = health_check_endpoints
                .split(',')
                .map(|address| format!("http://{}/metrics", address.trim()))
                .collect::<Vec<_>>();

            let mut previous_histogram_snapshots: HashMap<String, HistogramSnapshot> =
                HashMap::new();
            let scrapes = Self::get_scrapes(&metrics_addresses).await?;
            for (metrics_address, scrape) in scrapes {
                previous_histogram_snapshots.insert(
                    metrics_address,
                    Self::parse_histogram(&scrape, LATENCY_METRIC_PREFIX)?,
                );
            }

            let shutdown_notifier = shutdown_notifier.clone();
            let metrics_watcher: task::JoinHandle<Result<(), BenchmarkError>> = tokio::spawn(
                async move {
                    let mut health_interval = time::interval(time::Duration::from_secs(5));
                    let mut shutdown_interval = time::interval(time::Duration::from_secs(1));
                    loop {
                        tokio::select! {
                            biased;
                            _ = health_interval.tick() => {
                                let result = Self::validators_healthy(&metrics_addresses, &mut previous_histogram_snapshots).await;
                                if let Err(ref err) = result {
                                    info!("Shutting down benchmark due to error: {}", err);
                                    shutdown_notifier.cancel();
                                    break;
                                } else if !result? {
                                    info!("Shutting down benchmark due to unhealthy validators");
                                    shutdown_notifier.cancel();
                                    break;
                                }
                            }
                            _ = shutdown_interval.tick() => {
                                if shutdown_notifier.is_cancelled() {
                                    info!("Shutdown signal received, stopping metrics watcher");
                                    break;
                                }
                            }
                        }
                    }

                    Ok(())
                }
                .instrument(tracing::info_span!("metrics_watcher")),
            );

            Ok(Some(metrics_watcher))
        } else {
            Ok(None)
        }
    }

    fn runtime_control_task(
        shutdown_notifier: &CancellationToken,
        runtime_in_seconds: Option<u64>,
        num_chain_groups: usize,
    ) -> (Option<task::JoinHandle<()>>, Option<mpsc::Sender<()>>) {
        if let Some(runtime_in_seconds) = runtime_in_seconds {
            let (runtime_control_sender, mut runtime_control_receiver) =
                mpsc::channel(num_chain_groups);
            let shutdown_notifier = shutdown_notifier.clone();
            let runtime_control_task = task::spawn(
                async move {
                    let mut chains_started = 0;
                    while runtime_control_receiver.recv().await.is_some() {
                        chains_started += 1;
                        if chains_started == num_chain_groups {
                            break;
                        }
                    }
                    time::sleep(time::Duration::from_secs(runtime_in_seconds)).await;
                    shutdown_notifier.cancel();
                }
                .instrument(tracing::info_span!("runtime_control")),
            );
            (Some(runtime_control_task), Some(runtime_control_sender))
        } else {
            (None, None)
        }
    }

    async fn validators_healthy(
        metrics_addresses: &[String],
        previous_histogram_snapshots: &mut HashMap<String, HistogramSnapshot>,
    ) -> Result<bool, BenchmarkError> {
        let scrapes = Self::get_scrapes(metrics_addresses).await?;
        for (metrics_address, scrape) in scrapes {
            let histogram = Self::parse_histogram(&scrape, LATENCY_METRIC_PREFIX)?;
            let diff = Self::diff_histograms(
                previous_histogram_snapshots.get(&metrics_address).ok_or(
                    BenchmarkError::PreviousHistogramSnapshotDoesNotExist(metrics_address.clone()),
                )?,
                &histogram,
            )?;
            let p99 = match Self::compute_quantile(&diff.buckets, diff.count, 0.99) {
                Ok(p99) => p99,
                Err(BenchmarkError::NoDataYetForP99Calculation) => {
                    info!(
                        "No data available yet to calculate p99 for {}",
                        metrics_address
                    );
                    continue;
                }
                Err(e) => {
                    error!("Error computing p99 for {}: {}", metrics_address, e);
                    return Err(e);
                }
            };

            let last_bucket_boundary = diff.buckets[diff.buckets.len() - 2].less_than;
            if p99 == f64::INFINITY {
                info!(
                    "{} -> Estimated p99 for {} is higher than the last bucket boundary of {:?} ms",
                    metrics_address, LATENCY_METRIC_PREFIX, last_bucket_boundary
                );
            } else {
                info!(
                    "{} -> Estimated p99 for {}: {:.2} ms",
                    metrics_address, LATENCY_METRIC_PREFIX, p99
                );
            }
            if p99 > PROXY_LATENCY_P99_THRESHOLD {
                if p99 == f64::INFINITY {
                    error!(
                        "Proxy of validator {} unhealthy! Latency p99 is too high, it is higher than \
                        the last bucket boundary of {:.2} ms",
                        metrics_address, last_bucket_boundary
                    );
                } else {
                    error!(
                        "Proxy of validator {} unhealthy! Latency p99 is too high: {:.2} ms",
                        metrics_address, p99
                    );
                }
                return Ok(false);
            }
            previous_histogram_snapshots.insert(metrics_address.clone(), histogram);
        }

        Ok(true)
    }

    fn diff_histograms(
        previous: &HistogramSnapshot,
        current: &HistogramSnapshot,
    ) -> Result<HistogramSnapshot, BenchmarkError> {
        if current.count < previous.count {
            return Err(BenchmarkError::HistogramCountMismatch);
        }
        let total_diff = current.count - previous.count;
        let mut buckets_diff: Vec<HistogramCount> = Vec::new();
        for (before, after) in previous.buckets.iter().zip(current.buckets.iter()) {
            let bound_before = before.less_than;
            let bound_after = after.less_than;
            let cumulative_before = before.count;
            let cumulative_after = after.count;
            if (bound_before - bound_after).abs() > f64::EPSILON {
                return Err(BenchmarkError::BucketBoundariesDoNotMatch(
                    bound_before,
                    bound_after,
                ));
            }
            let diff = (cumulative_after - cumulative_before).max(0.0);
            buckets_diff.push(HistogramCount {
                less_than: bound_after,
                count: diff,
            });
        }
        Ok(HistogramSnapshot {
            buckets: buckets_diff,
            count: total_diff,
            sum: current.sum - previous.sum,
        })
    }

    async fn get_scrapes(
        metrics_addresses: &[String],
    ) -> Result<Vec<(String, Scrape)>, BenchmarkError> {
        let mut scrapes = Vec::new();
        for metrics_address in metrics_addresses {
            let response = reqwest::get(metrics_address)
                .await
                .map_err(BenchmarkError::Reqwest)?;
            let metrics = response.text().await.map_err(BenchmarkError::Reqwest)?;
            let scrape = Scrape::parse(metrics.lines().map(|line| Ok(line.to_owned())))
                .map_err(BenchmarkError::IoError)?;
            scrapes.push((metrics_address.clone(), scrape));
        }
        Ok(scrapes)
    }

    fn parse_histogram(
        scrape: &Scrape,
        metric_prefix: &str,
    ) -> Result<HistogramSnapshot, BenchmarkError> {
        let mut buckets: Vec<HistogramCount> = Vec::new();
        let mut total_count: Option<f64> = None;
        let mut total_sum: Option<f64> = None;

        // Iterate over each metric in the scrape.
        for sample in &scrape.samples {
            if sample.metric == metric_prefix {
                if let Value::Histogram(histogram) = &sample.value {
                    buckets.extend(histogram.iter().cloned());
                } else {
                    return Err(BenchmarkError::ExpectedHistogramValue(sample.value.clone()));
                }
            } else if sample.metric == format!("{}_count", metric_prefix) {
                if let Value::Untyped(count) = sample.value {
                    total_count = Some(count);
                } else {
                    return Err(BenchmarkError::ExpectedUntypedValue(sample.value.clone()));
                }
            } else if sample.metric == format!("{}_sum", metric_prefix) {
                if let Value::Untyped(sum) = sample.value {
                    total_sum = Some(sum);
                } else {
                    return Err(BenchmarkError::ExpectedUntypedValue(sample.value.clone()));
                }
            }
        }

        match (total_count, total_sum) {
            (Some(count), Some(sum)) if !buckets.is_empty() => {
                buckets.sort_by(|a, b| {
                    a.less_than
                        .partial_cmp(&b.less_than)
                        .expect("Comparison should not fail")
                });
                Ok(HistogramSnapshot {
                    buckets,
                    count,
                    sum,
                })
            }
            _ => Err(BenchmarkError::IncompleteHistogramData),
        }
    }

    fn compute_quantile(
        buckets: &[HistogramCount],
        total_count: f64,
        quantile: f64,
    ) -> Result<f64, BenchmarkError> {
        if total_count == 0.0 {
            // Had no samples in the last 5s.
            return Err(BenchmarkError::NoDataYetForP99Calculation);
        }
        // Compute the target cumulative count.
        let target = (quantile * total_count).ceil();
        let mut prev_cumulative = 0.0;
        let mut prev_bound = 0.0;
        for bucket in buckets {
            if bucket.count >= target {
                let bucket_count = bucket.count - prev_cumulative;
                if bucket_count == 0.0 {
                    // Bucket that is supposed to contain the target quantile is empty, unexpectedly.
                    return Err(BenchmarkError::UnexpectedEmptyBucket);
                }
                let fraction = (target - prev_cumulative) / bucket_count;
                return Ok(prev_bound + (bucket.less_than - prev_bound) * fraction);
            }
            prev_cumulative = bucket.count;
            prev_bound = bucket.less_than;
        }
        Err(BenchmarkError::CouldNotComputeQuantile)
    }

    #[expect(clippy::too_many_arguments)]
    async fn run_benchmark_internal(
        chain_idx: usize,
        chain_id: ChainId,
        bps: usize,
        chain_client: ChainClient<Env>,
        all_chains: Vec<ChainId>,
        transactions_per_block: usize,
        mode: BenchmarkMode,
        shutdown_notifier: CancellationToken,
        bps_count: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
        notifier: Arc<Notify>,
        runtime_control_sender: Option<mpsc::Sender<()>>,
        delay_between_chains_ms: Option<u64>,
        single_destination_per_block: bool,
    ) -> Result<(), BenchmarkError> {
        barrier.wait().await;
        if let Some(delay_between_chains_ms) = delay_between_chains_ms {
            time::sleep(time::Duration::from_millis(
                (chain_idx as u64) * delay_between_chains_ms,
            ))
            .await;
        }
        info!("Starting benchmark for chain {:?}", chain_id);

        if let Some(runtime_control_sender) = runtime_control_sender {
            runtime_control_sender.send(()).await?;
        }

        let owner = chain_client
            .identity()
            .await
            .map_err(BenchmarkError::ChainClient)?;
        let mut operation_generator = BenchmarkOperationGenerator::new(chain_id, all_chains, mode)?;

        loop {
            tokio::select! {
                biased;

                _ = shutdown_notifier.cancelled() => {
                    info!("Shutdown signal received, stopping benchmark");
                    break;
                }
                result = chain_client.execute_operations(
                    operation_generator.generate_operations(
                        owner,
                        transactions_per_block,
                        single_destination_per_block,
                    ),
                    vec![]
                ) => {
                    result
                        .map_err(BenchmarkError::ChainClient)?
                        .expect("should execute block with operations");

                    let current_bps_count = bps_count.fetch_add(1, Ordering::Relaxed) + 1;
                    if current_bps_count >= bps {
                        notifier.notified().await;
                    }
                }
            }
        }

        info!("Exiting task...");
        Ok(())
    }

    /// Closes the chain that was created for the benchmark.
    pub async fn close_benchmark_chain(
        chain_client: &ChainClient<Env>,
    ) -> Result<(), BenchmarkError> {
        let start = Instant::now();
        chain_client
            .execute_operation(Operation::system(SystemOperation::CloseChain))
            .await?
            .expect("Close chain operation should not fail!");

        debug!(
            "Closed chain {:?} in {} ms",
            chain_client.chain_id(),
            start.elapsed().as_millis()
        );

        Ok(())
    }

    pub fn get_all_chains(
        chains_config_path: Option<&Path>,
        benchmark_chains: &[(ChainId, AccountOwner)],
    ) -> Result<Vec<ChainId>, BenchmarkError> {
        let all_chains = if let Some(config_path) = chains_config_path {
            if !config_path.exists() {
                return Err(BenchmarkError::ConfigFileNotFound(
                    config_path.to_path_buf(),
                ));
            }
            let config = BenchmarkConfig::load_from_file(config_path)
                .map_err(BenchmarkError::ConfigLoadError)?;
            config.chain_ids
        } else {
            benchmark_chains.iter().map(|(id, _)| *id).collect()
        };

        Ok(all_chains)
    }
}

/// Creates a fungible token transfer operation.
pub fn fungible_transfer(
    application_id: ApplicationId,
    chain_id: ChainId,
    sender: AccountOwner,
    receiver: AccountOwner,
    amount: Amount,
) -> Operation {
    let target_account = fungible::Account {
        chain_id,
        owner: receiver,
    };
    let bytes = bcs::to_bytes(&FungibleOperation::Transfer {
        owner: sender,
        amount,
        target_account,
    })
    .expect("should serialize fungible token operation");
    Operation::User {
        application_id,
        bytes,
    }
}

/// Manages operation generation for benchmarks, including destination selection and mode-specific logic.
struct BenchmarkOperationGenerator {
    source_chain_id: ChainId,
    destination_index: usize,
    destination_chains: Vec<ChainId>,
    rng: SmallRng,
    mode: BenchmarkMode,
    pm_generator: Option<PmOrderGenerator>,
}

impl BenchmarkOperationGenerator {
    fn new(
        source_chain_id: ChainId,
        mut destination_chains: Vec<ChainId>,
        mode: BenchmarkMode,
    ) -> Result<Self, BenchmarkError> {
        let mut rng = SmallRng::from_rng(thread_rng())?;
        destination_chains.shuffle(&mut rng);

        let pm_generator = match &mode {
            BenchmarkMode::Pm {
                pattern,
                price_scale,
                ..
            } => Some(PmOrderGenerator::new(*pattern, *price_scale)),
            _ => None,
        };

        Ok(Self {
            source_chain_id,
            destination_index: 0,
            destination_chains,
            rng,
            mode,
            pm_generator,
        })
    }

    /// Get the next destination chain. For PM mode, always returns the market chain.
    fn get_next_destination(&mut self) -> ChainId {
        // For PM mode, destination is always the market chain
        if let Some(market_chain) = self.mode.market_chain_id() {
            return market_chain;
        }

        // Check if we've gone through all destinations
        if self.destination_index >= self.destination_chains.len() {
            // Reshuffle the destinations for the next cycle
            self.destination_chains.shuffle(&mut self.rng);
            self.destination_index = 0;
        }

        let destination_chain_id = self.destination_chains[self.destination_index];
        self.destination_index += 1;

        if destination_chain_id == self.source_chain_id {
            self.get_next_destination()
        } else {
            destination_chain_id
        }
    }

    /// Generate operations for a single block.
    fn generate_operations(
        &mut self,
        owner: AccountOwner,
        transactions_per_block: usize,
        single_destination_per_block: bool,
    ) -> Vec<Operation> {
        let amount = Amount::from_attos(1);

        if single_destination_per_block {
            let recipient_chain_id = self.get_next_destination();
            (0..transactions_per_block)
                .map(|_| self.create_operation(recipient_chain_id, owner, amount))
                .collect()
        } else {
            (0..transactions_per_block)
                .map(|_| {
                    let recipient_chain_id = self.get_next_destination();
                    self.create_operation(recipient_chain_id, owner, amount)
                })
                .collect()
        }
    }

    /// Create a single operation based on the benchmark mode.
    fn create_operation(
        &mut self,
        recipient_chain_id: ChainId,
        owner: AccountOwner,
        amount: Amount,
    ) -> Operation {
        match &self.mode {
            BenchmarkMode::NativeTransfer => Operation::system(SystemOperation::Transfer {
                owner: AccountOwner::CHAIN,
                recipient: Account::chain(recipient_chain_id),
                amount,
            }),
            BenchmarkMode::FungibleTransfer { application_id } => {
                fungible_transfer(*application_id, recipient_chain_id, owner, owner, amount)
            }
            BenchmarkMode::Pm { pm_engine_id, .. } => {
                let generator = self
                    .pm_generator
                    .as_mut()
                    .expect("PM generator should be initialized for PM mode");
                let operation = generator.next_order(&mut self.rng, owner);
                create_pm_operation(*pm_engine_id, operation)
            }
        }
    }
}
