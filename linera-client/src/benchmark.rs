// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

pub mod rate;

/// Per-interval latency, shared between the workers that measure it and the controller that
/// reads it.
///
/// One shared `Mutex<Histogram>`, taken once per committed block. Sharding per worker really is
/// 5-10x faster on this line (measured: 22.7M vs 229M records/s at 32 threads), but the shared
/// path still has ~3 orders of magnitude of headroom over any block rate one client can drive,
/// so the ceiling is the commit path, not this lock.
#[derive(Clone)]
pub struct LatencyRecorder {
    histogram: Arc<std::sync::Mutex<hdrhistogram::Histogram<u64>>>,
}

impl LatencyRecorder {
    /// Records microseconds, covering 1us to 5 minutes at three significant figures.
    pub fn new() -> Self {
        let histogram = hdrhistogram::Histogram::new_with_bounds(1, 300_000_000, 3)
            .expect("valid histogram bounds");
        Self {
            histogram: Arc::new(std::sync::Mutex::new(histogram)),
        }
    }

    /// Adds one committed block's latency.
    pub fn record(&self, elapsed: std::time::Duration) {
        let micros = elapsed.as_micros().min(u64::MAX as u128) as u64;
        if let Ok(mut histogram) = self.histogram.lock() {
            // Saturating rather than plain `record`: a block slower than the upper bound is
            // still a data point, and dropping it would flatter the tail.
            histogram.saturating_record(micros);
        }
    }

    /// Returns the p99 and its sample count, draining the window — but only once `min_samples`
    /// blocks back it; under the floor the histogram is left intact so the window widens instead.
    /// Draining once supported is what lets the tail recover after the search overshoots.
    pub fn take_p99(&self, min_samples: u64) -> Option<(std::time::Duration, u64)> {
        let mut histogram = self.histogram.lock().ok()?;
        let count = histogram.len();
        if count < min_samples.max(1) {
            return None;
        }
        let p99 = histogram.value_at_quantile(0.99);
        histogram.reset();
        Some((std::time::Duration::from_micros(p99), count))
    }
}

impl Default for LatencyRecorder {
    fn default() -> Self {
        Self::new()
    }
}

use std::{
    collections::HashMap,
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use linera_base::{
    data_types::{Amount, Timestamp},
    identifiers::{Account, AccountOwner, ApplicationId, ChainId},
    time::Instant,
};
use linera_core::{
    client::chain_client::{self, ChainClient},
    data_types::ClientOutcome,
    Environment,
};
use linera_execution::{system::SystemOperation, Operation};
use linera_sdk::abis::fungible::FungibleOperation;
use num_format::{Locale, ToFormattedString};
use prometheus_parse::{HistogramCount, Scrape, Value};
use rand::{rngs::SmallRng, seq::SliceRandom, thread_rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{mpsc, Barrier, Notify},
    task, time,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn, Instrument as _};

use crate::chain_listener::{ChainListener, ClientContext};

/// Trait for generating benchmark operations.
///
/// Implement this trait to create custom operation generators for different
/// application benchmarks (e.g., prediction markets, custom tokens, etc.).
///
/// Each benchmark chain gets its own generator instance. The generator is responsible
/// for producing operations to include in blocks, including any destination chain
/// selection logic.
pub trait OperationGenerator: Send + 'static {
    /// Generate a batch of operations for a single block.
    fn generate_operations(&mut self, owner: AccountOwner, count: usize) -> Vec<Operation>;
}

/// A client the benchmark can drive, so the same harness runs against either the full
/// [`ChainClient`] or a storage-free proposer.
///
/// The benchmark loop only ever asks a client to commit one block of operations, which is
/// what makes the two interchangeable: everything else -- rate control, block sizing,
/// destination selection, reporting -- is the harness's job and is shared.
#[cfg_attr(not(web), async_trait::async_trait)]
#[cfg_attr(web, async_trait::async_trait(?Send))]
pub trait BenchmarkClient: Send + Sync + 'static {
    /// The chain this client proposes on.
    fn chain_id(&self) -> ChainId;

    /// The owner the generated operations are attributed to.
    async fn owner(&self) -> Result<AccountOwner, BenchmarkError>;

    /// Proposes a block carrying `operations` and returns once it is committed.
    async fn commit_operations(&self, operations: Vec<Operation>) -> Result<(), BenchmarkError>;
}

#[cfg_attr(not(web), async_trait::async_trait)]
#[cfg_attr(web, async_trait::async_trait(?Send))]
impl<Env: Environment> BenchmarkClient for ChainClient<Env> {
    fn chain_id(&self) -> ChainId {
        ChainClient::chain_id(self)
    }

    async fn owner(&self) -> Result<AccountOwner, BenchmarkError> {
        self.identity().await.map_err(BenchmarkError::ChainClient)
    }

    async fn commit_operations(&self, operations: Vec<Operation>) -> Result<(), BenchmarkError> {
        self.execute_operations(operations, vec![])
            .await
            .map_err(BenchmarkError::ChainClient)?
            .expect("should execute block with operations");
        Ok(())
    }
}

/// Generates native fungible token transfer operations between chains.
pub struct NativeFungibleTransferGenerator {
    source_chain_id: ChainId,
    destination_chains: Vec<ChainId>,
    destination_index: usize,
    rng: SmallRng,
    single_destination_per_block: bool,
    avoid_self: bool,
}

impl NativeFungibleTransferGenerator {
    /// Creates a generator that sends native token transfers from the source chain.
    ///
    /// If `avoid_self` is true, `self.source_chain_id` is skipped whenever the destination
    /// list has more than one entry (the historical behavior: a caller that wants a mix of
    /// self- and cross-chain traffic should build a destination list that already includes
    /// `source_chain_id` explicitly and pass `avoid_self = false`, otherwise it would never
    /// actually be selected).
    pub fn new(
        source_chain_id: ChainId,
        mut destination_chains: Vec<ChainId>,
        single_destination_per_block: bool,
        avoid_self: bool,
    ) -> Result<Self, BenchmarkError> {
        // With a single chain, send to self.
        if destination_chains.is_empty() {
            destination_chains.push(source_chain_id);
        }
        let mut rng = SmallRng::from_rng(thread_rng())?;
        destination_chains.shuffle(&mut rng);
        Ok(Self {
            source_chain_id,
            destination_chains,
            destination_index: 0,
            rng,
            single_destination_per_block,
            avoid_self,
        })
    }

    fn next_destination(&mut self) -> ChainId {
        if self.destination_index >= self.destination_chains.len() {
            self.destination_chains.shuffle(&mut self.rng);
            self.destination_index = 0;
        }
        let destination_chain_id = self.destination_chains[self.destination_index];
        self.destination_index += 1;
        // Skip self when there are other destinations available.
        if destination_chain_id == self.source_chain_id
            && self.destination_chains.len() > 1
            && self.avoid_self
        {
            self.next_destination()
        } else {
            destination_chain_id
        }
    }
}

impl OperationGenerator for NativeFungibleTransferGenerator {
    fn generate_operations(&mut self, _owner: AccountOwner, count: usize) -> Vec<Operation> {
        let amount = Amount::from_attos(1);
        if self.single_destination_per_block {
            let recipient = self.next_destination();
            (0..count)
                .map(|_| {
                    Operation::system(SystemOperation::Transfer {
                        owner: AccountOwner::CHAIN,
                        recipient: Account::chain(recipient),
                        amount,
                    })
                })
                .collect()
        } else {
            (0..count)
                .map(|_| {
                    let recipient = self.next_destination();
                    Operation::system(SystemOperation::Transfer {
                        owner: AccountOwner::CHAIN,
                        recipient: Account::chain(recipient),
                        amount,
                    })
                })
                .collect()
        }
    }
}

/// Generates fungible token transfer operations between chains.
pub struct FungibleTransferGenerator {
    application_id: ApplicationId,
    source_chain_id: ChainId,
    destination_chains: Vec<ChainId>,
    destination_index: usize,
    rng: SmallRng,
    single_destination_per_block: bool,
    avoid_self: bool,
}

impl FungibleTransferGenerator {
    /// Creates a generator that sends fungible token transfers from the source chain.
    ///
    /// `avoid_self` has the same meaning as on [`NativeFungibleTransferGenerator::new`]: with
    /// it set, `source_chain_id` is skipped whenever the destination list has more than one
    /// entry, so a caller wanting a mix of self- and cross-chain traffic passes `false` and a
    /// list that already contains `source_chain_id`.
    pub fn new(
        application_id: ApplicationId,
        source_chain_id: ChainId,
        mut destination_chains: Vec<ChainId>,
        single_destination_per_block: bool,
        avoid_self: bool,
    ) -> Result<Self, BenchmarkError> {
        // With a single chain, send to self (matching old behavior).
        if destination_chains.is_empty() {
            destination_chains.push(source_chain_id);
        }
        let mut rng = SmallRng::from_rng(thread_rng())?;
        destination_chains.shuffle(&mut rng);
        Ok(Self {
            application_id,
            source_chain_id,
            destination_chains,
            destination_index: 0,
            rng,
            single_destination_per_block,
            avoid_self,
        })
    }

    fn next_destination(&mut self) -> ChainId {
        if self.destination_index >= self.destination_chains.len() {
            self.destination_chains.shuffle(&mut self.rng);
            self.destination_index = 0;
        }
        let destination_chain_id = self.destination_chains[self.destination_index];
        self.destination_index += 1;
        // Skip self when there are other destinations available.
        if destination_chain_id == self.source_chain_id
            && self.destination_chains.len() > 1
            && self.avoid_self
        {
            self.next_destination()
        } else {
            destination_chain_id
        }
    }
}

impl OperationGenerator for FungibleTransferGenerator {
    fn generate_operations(&mut self, owner: AccountOwner, count: usize) -> Vec<Operation> {
        let amount = Amount::from_attos(1);
        if self.single_destination_per_block {
            let recipient = self.next_destination();
            (0..count)
                .map(|_| fungible_transfer(self.application_id, recipient, owner, owner, amount))
                .collect()
        } else {
            (0..count)
                .map(|_| {
                    let recipient = self.next_destination();
                    fungible_transfer(self.application_id, recipient, owner, owner, amount)
                })
                .collect()
        }
    }
}

const PROXY_LATENCY_P99_THRESHOLD: f64 = 400.0;
const LATENCY_METRIC_PREFIX: &str = "linera_proxy_request_latency";

/// An error that can occur while running a benchmark.
#[derive(Debug, thiserror::Error)]
#[allow(missing_docs)]
pub enum BenchmarkError {
    #[error("Failed to join task: {0}")]
    JoinError(#[from] task::JoinError),
    #[error("Chain client error: {0}")]
    ChainClient(#[from] chain_client::Error),
    /// The storage-free client has no `chain_client::Error` to wrap, so its failures arrive
    /// as a message.
    #[error("Lite client error: {0}")]
    LiteClient(String),
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
    #[error("Chain listener startup error")]
    ChainListenerStartupError,
}

#[derive(Debug)]
struct HistogramSnapshot {
    buckets: Vec<HistogramCount>,
    count: f64,
    sum: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
/// Configuration listing the chains to use for a benchmark.
pub struct BenchmarkConfig {
    /// The chains to use for the benchmark.
    pub chain_ids: Vec<ChainId>,
}

impl BenchmarkConfig {
    /// Loads the benchmark configuration from a YAML file.
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config = serde_yaml::from_str(&content)?;
        Ok(config)
    }

    /// Saves the benchmark configuration to a YAML file.
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = serde_yaml::to_string(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }
}

/// Driver for running benchmarks against a network.
pub struct Benchmark<Env: Environment> {
    _phantom: std::marker::PhantomData<Env>,
}

impl<Env: Environment> Benchmark<Env> {
    /// Runs a benchmark with the given chain clients and operation generators.
    ///
    /// Each chain client is paired with an operation generator (one per chain).
    /// The generators produce the operations to include in each block.
    #[expect(clippy::too_many_arguments)]
    pub async fn run_benchmark<C: ClientContext<Environment = Env> + 'static>(
        bps: usize,
        chain_clients: Vec<Arc<dyn BenchmarkClient>>,
        generators: Vec<Box<dyn OperationGenerator>>,
        transactions_per_block: usize,
        health_check_endpoints: Option<String>,
        runtime_in_seconds: Option<u64>,
        delay_between_chains_ms: Option<u64>,
        chain_listener: Option<ChainListener<C>>,
        rate_search: Option<rate::RateSearch>,
        rate_control: rate::RateControlConfig,
        shutdown_notifier: &CancellationToken,
    ) -> Result<(), BenchmarkError> {
        assert_eq!(
            chain_clients.len(),
            generators.len(),
            "Must have one generator per chain client"
        );
        let num_chains = chain_clients.len();
        let bps_counts = (0..num_chains)
            .map(|_| Arc::new(AtomicUsize::new(0)))
            .collect::<Vec<_>>();
        let notifier = Arc::new(Notify::new());
        let barrier = Arc::new(Barrier::new(num_chains + 1));

        // Only the full client needs it: it keeps local chain state in sync in the
        // background. The storage-free client has no local state to sync, and running one
        // anyway would put exactly the work it avoids back onto the load generator.
        let chain_listener_handle = match chain_listener {
            Some(chain_listener) => {
                let future = chain_listener
                    .run()
                    .await
                    .map_err(|_| BenchmarkError::ChainListenerStartupError)?;
                Some(tokio::spawn(future.in_current_span()))
            }
            None => None,
        };

        // Captured before `rate_search` is moved into the controller. Workers need it to know
        // whether a failed commit is an overshoot to report or a genuine error to raise.
        let rate_auto = rate_search.is_some();

        // One share per chain, held in an atomic so `--rate-auto` can move the whole fleet's
        // target mid-run: the controller rewrites these, the workers re-read them per block.
        // Seeded from the search's own starting target when there is one: `--bps` is the fixed
        // rate, `--rate-start-bps` is where the climb opens, and seeding from the wrong one
        // makes the first window measure a rate the search does not think it asked for.
        let initial_bps = rate_search
            .as_ref()
            .map_or(bps, |search| search.target_bps());
        let bps_shares: Vec<Arc<AtomicUsize>> = Self::split_bps(initial_bps, num_chains);
        let latencies = LatencyRecorder::new();

        let bps_control_task = Self::bps_control_task(
            &barrier,
            shutdown_notifier,
            &bps_counts,
            &notifier,
            transactions_per_block,
            initial_bps,
            bps_shares.clone(),
            latencies.clone(),
            rate_search,
            num_chains,
            delay_between_chains_ms,
            rate_control,
        );

        let (runtime_control_task, runtime_control_sender) =
            Self::runtime_control_task(shutdown_notifier, runtime_in_seconds, num_chains);

        let mut join_set = task::JoinSet::<Result<(), BenchmarkError>>::new();
        for (chain_idx, (chain_client, generator)) in
            chain_clients.into_iter().zip(generators).enumerate()
        {
            let chain_id = chain_client.chain_id();
            let shutdown_notifier_clone = shutdown_notifier.clone();
            let barrier_clone = barrier.clone();
            let bps_count_clone = bps_counts[chain_idx].clone();
            let notifier_clone = notifier.clone();
            let runtime_control_sender_clone = runtime_control_sender.clone();
            let bps_share = bps_shares[chain_idx].clone();
            let latencies_clone = latencies.clone();
            join_set.spawn(
                async move {
                    Box::pin(Self::run_benchmark_internal(
                        chain_idx,
                        chain_id,
                        bps_share,
                        latencies_clone,
                        chain_client,
                        generator,
                        transactions_per_block,
                        shutdown_notifier_clone,
                        bps_count_clone,
                        barrier_clone,
                        notifier_clone,
                        runtime_control_sender_clone,
                        delay_between_chains_ms,
                        rate_auto,
                        rate_control.max_commit_failure_secs,
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

        // Both figures, always: `offered` is what the search bracketed on, `achieved` is what
        // the network actually committed, and a level is confirmed while delivering as little
        // as 80% of its target. Reporting only the offered rate overstates throughput.
        match bps_control_task.await? {
            // Zero is the absence of a measurement, not a measurement of zero: the search
            // bracketed all the way down without any rate holding the budget. Logging it on
            // the success line would read as a knee of 0 rather than as no knee at all.
            Some(rate::SearchOutcome::Converged(knee)) if knee.offered_bps == 0 => warn!(
                "rate search found NO sustainable rate: every rate tried, down to the lowest \
                 the generators can offer, missed the latency budget or was not delivered"
            ),
            Some(rate::SearchOutcome::Converged(knee)) => info!(
                knee_bps = knee.offered_bps,
                knee_achieved_bps = knee.achieved_bps,
                knee_achieved_tps = knee.achieved_bps * transactions_per_block as f64,
                "rate search converged; the highest rate that held the latency budget"
            ),
            Some(rate::SearchOutcome::CutShort(best)) => warn!(
                best_bps = best.offered_bps,
                best_achieved_bps = best.achieved_bps,
                best_achieved_tps = best.achieved_bps * transactions_per_block as f64,
                "rate search cut short by the runtime limit; this is a LOWER BOUND on the knee, \
                 not the knee"
            ),
            None => {}
        }
        if let Some(metrics_watcher) = metrics_watcher {
            metrics_watcher.await??;
        }
        if let Some(runtime_control_task) = runtime_control_task {
            runtime_control_task.await?;
        }

        if let Some(chain_listener_handle) = chain_listener_handle {
            if let Err(e) = chain_listener_handle.await? {
                tracing::error!("chain listener error: {e}");
            }
        }

        Ok(())
    }

    // The bps control task will control the BPS from the threads.
    /// Splits a fleet-wide target into per-chain shares, distributing the remainder so the
    /// shares sum to exactly `bps` rather than losing up to `num_chains - 1` to truncation.
    fn split_bps(bps: usize, num_chains: usize) -> Vec<Arc<AtomicUsize>> {
        let base = bps / num_chains;
        let remainder = bps % num_chains;
        (0..num_chains)
            .map(|i| {
                Arc::new(AtomicUsize::new(if i < remainder {
                    base + 1
                } else {
                    base
                }))
            })
            .collect()
    }

    /// Rewrites the shares in place for a new fleet-wide target.
    fn set_bps(shares: &[Arc<AtomicUsize>], bps: usize) {
        let base = bps / shares.len();
        let remainder = bps % shares.len();
        for (i, share) in shares.iter().enumerate() {
            let value = if i < remainder { base + 1 } else { base };
            share.store(value, Ordering::Relaxed);
        }
    }

    #[expect(clippy::too_many_arguments)]
    fn bps_control_task(
        barrier: &Arc<Barrier>,
        shutdown_notifier: &CancellationToken,
        bps_counts: &[Arc<AtomicUsize>],
        notifier: &Arc<Notify>,
        transactions_per_block: usize,
        initial_bps: usize,
        bps_shares: Vec<Arc<AtomicUsize>>,
        latencies: LatencyRecorder,
        mut search: Option<rate::RateSearch>,
        num_chains: usize,
        delay_between_chains_ms: Option<u64>,
        rate_control: rate::RateControlConfig,
    ) -> task::JoinHandle<Option<rate::SearchOutcome>> {
        let shutdown_notifier = shutdown_notifier.clone();
        let bps_counts = bps_counts.to_vec();
        let notifier = notifier.clone();
        let barrier = barrier.clone();
        task::spawn(
            async move {
                barrier.wait().await;
                let mut one_second_interval = time::interval(time::Duration::from_secs(1));
                // The rate the fleet is currently being asked for, and the same value
                // `bps_shares` was seeded with. Under `--rate-auto` the search moves it every
                // window; seeding it from `--bps` instead would make the shortfall warning
                // name a rate the fleet was never driven at.
                let mut current_target = initial_bps;
                // When the window being assembled started. At low rates one second holds too
                // few samples for a p99, so a window can span several ticks.
                let mut window_start = time::Instant::now();
                // Workers sleep their stagger AFTER this barrier, so the fleet is not complete
                // until the last one wakes; judging before then measures the ramp, not the
                // network.
                let ramp_ms =
                    delay_between_chains_ms.unwrap_or(0) * (num_chains.saturating_sub(1)) as u64;
                let settle_until = time::Instant::now()
                    + time::Duration::from_millis(ramp_ms)
                    + time::Duration::from_secs(rate_control.settle_secs);
                let mut converged = None;
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
                        (current_target * transactions_per_block).to_formatted_string(&Locale::en);
                    let formatted_bps_goal = current_target.to_formatted_string(&Locale::en);
                    if current_bps_count >= current_target {
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

                    // `--rate-auto`: feed the window to the search and move the fleet to
                    // whatever it asks for next.
                    if let Some(search) = search.as_mut() {
                        if time::Instant::now() < settle_until {
                            // Drain rather than skip: warm-up latencies must not leak into the
                            // first judged window.
                            latencies.take_p99(1);
                            window_start = time::Instant::now();
                            continue;
                        }
                        // Wait for enough samples to support a p99, but not forever: a slow
                        // network never reaches the floor, and a stalled search measures nothing.
                        let aged_out =
                            window_start.elapsed().as_secs() >= rate_control.max_window_secs;
                        let floor = if aged_out {
                            1
                        } else {
                            rate_control.min_p99_samples
                        };
                        let (p99, samples) = match latencies.take_p99(floor) {
                            Some(measured) => measured,
                            // An aged-out window with NO committed blocks is the most important
                            // observation there is: every commit failed, so there are no
                            // latencies to summarise. Skipping it leaves the search blind to
                            // the one condition it must react to -- it would hold the
                            // unservable rate forever, never learning to back off.
                            None if aged_out => (window_start.elapsed(), 0),
                            None => continue,
                        };
                        // Rate over the SAME window the p99 came from, counted from COMMITTED
                        // blocks: `samples` is the histogram's length, and the histogram is
                        // written only on success. The per-chain counters tick on every
                        // attempt, so using them would score a rate whose commits are failing
                        // as fully delivered and climb straight past the ceiling.
                        let elapsed = window_start.elapsed().as_secs_f64().max(f64::EPSILON);
                        let observation = rate::Observation {
                            achieved_bps: samples as f64 / elapsed,
                            p99,
                        };
                        window_start = time::Instant::now();
                        match search.observe(observation) {
                            rate::Decision::Hold(target) => {
                                Self::set_bps(&bps_shares, target);
                                current_target = target;
                                info!(
                                    target_bps = target,
                                    p99_ms = p99.as_millis() as u64,
                                    achieved_bps = observation.achieved_bps,
                                    samples = samples,
                                    window_s = elapsed,
                                    "rate search"
                                );
                            }
                            rate::Decision::Converged { .. } => {
                                // From the search, not the decision: it carries the delivered
                                // rate alongside the offered one.
                                converged = Some(search.best_so_far());
                                shutdown_notifier.cancel();
                                break;
                            }
                        }
                    }
                }

                info!("Exiting bps control task");
                // Returned rather than logged in-task: the runtime limit cancels, the workers
                // stop and the process unwinds, so anything emitted from here races the exit.
                // Converged and cut-short stay distinct: a lower bound reported as a knee is
                // the failure the e2e assertion exists to catch.
                match converged {
                    Some(knee) => Some(rate::SearchOutcome::Converged(knee)),
                    None => search
                        .as_ref()
                        .map(|search| rate::SearchOutcome::CutShort(search.best_so_far())),
                }
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
                    // Raced against the token: under `--rate-auto` an early exit is the NORMAL
                    // ending -- convergence cancels -- and a bare sleep here would hold the run
                    // open, chains and all, for the rest of `--runtime-in-seconds` after the
                    // knee had already been reported.
                    tokio::select! {
                        _ = time::sleep(time::Duration::from_secs(runtime_in_seconds)) => {
                            shutdown_notifier.cancel();
                        }
                        _ = shutdown_notifier.cancelled() => {}
                    }
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
            } else if sample.metric == format!("{metric_prefix}_count") {
                if let Value::Untyped(count) = sample.value {
                    total_count = Some(count);
                } else {
                    return Err(BenchmarkError::ExpectedUntypedValue(sample.value.clone()));
                }
            } else if sample.metric == format!("{metric_prefix}_sum") {
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
        bps_share: Arc<AtomicUsize>,
        latencies: LatencyRecorder,
        chain_client: Arc<dyn BenchmarkClient>,
        mut generator: Box<dyn OperationGenerator>,
        transactions_per_block: usize,
        shutdown_notifier: CancellationToken,
        bps_count: Arc<AtomicUsize>,
        barrier: Arc<Barrier>,
        notifier: Arc<Notify>,
        runtime_control_sender: Option<mpsc::Sender<()>>,
        delay_between_chains_ms: Option<u64>,
        rate_auto: bool,
        max_commit_failure_secs: u64,
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

        let owner = chain_client.owner().await?;
        // When the current unbroken run of commit failures began; cleared by any success.
        let mut failing_since: Option<Instant> = None;

        loop {
            // Deliberately NOT raced against the shutdown signal. `select!` drops the losing
            // future, and dropping a commit mid-flight abandons a block the validators have
            // already voted on: the storage-free client keeps no local record of it, so the
            // chain is left with an uncertified proposal at that height and every later
            // proposal there is rejected with "Already voted to confirm a different block".
            // Finishing the block first costs at most one block of shutdown latency.
            if shutdown_notifier.is_cancelled() {
                info!("Shutdown signal received, stopping benchmark");
                break;
            }

            let started = Instant::now();
            let commit = chain_client
                .commit_operations(generator.generate_operations(owner, transactions_per_block))
                .await;
            match commit {
                Ok(()) => {
                    failing_since = None;
                    latencies.record(started.elapsed());
                }
                // The search has to overshoot to bracket the knee, so a commit that fails at
                // the overshoot rate is the measurement: it lands as missing throughput and
                // the rate is judged unsustained. Only meaningful under `--rate-auto`, which
                // will come back down; at a fixed rate there is nothing to back off to.
                Err(error) if rate_auto => {
                    let since = *failing_since.get_or_insert_with(Instant::now);
                    let failing_for = since.elapsed();
                    warn!(
                        %error,
                        failing_for_ms = failing_for.as_millis() as u64,
                        "commit failed; counting this rate as unsustained"
                    );
                    // A chain wedged by an uncertified proposal fails identically to an
                    // overshoot but never recovers. Only a streak that outlasts the
                    // controller's back-off distinguishes the two.
                    //
                    // Ending the run rather than returning Err: by this point the search has
                    // usually confirmed a knee, and that measurement is the whole point of the
                    // run. Erroring out discards it and reports nothing at all, which is
                    // strictly less useful than the number plus a loud warning about why the
                    // run stopped early.
                    if failing_for >= std::time::Duration::from_secs(max_commit_failure_secs) {
                        error!(
                            %error,
                            failing_for_ms = failing_for.as_millis() as u64,
                            "chain has failed every commit for the whole bound and is not \
                             recovering; ending the run so the rate confirmed so far is still \
                             reported"
                        );
                        shutdown_notifier.cancel();
                        break;
                    }
                }
                Err(error) => return Err(error),
            }

            // Read the share fresh each block: under `--rate-auto` the controller moves the
            // target while the run is in flight, and a share captured at startup would pin
            // every worker to the rate the search began at.
            let share = bps_share.load(Ordering::Relaxed);
            let current_bps_count = bps_count.fetch_add(1, Ordering::Relaxed) + 1;
            if current_bps_count >= share {
                // Safe to race: waiting on the notifier holds no chain state, and it would
                // otherwise block until the next tick even after shutdown.
                tokio::select! {
                    biased;

                    _ = shutdown_notifier.cancelled() => {
                        info!("Shutdown signal received, stopping benchmark");
                        break;
                    }
                    _ = notifier.notified() => {}
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
        loop {
            let result = chain_client
                .execute_operation(Operation::system(SystemOperation::CloseChain))
                .await?;
            match result {
                ClientOutcome::Committed(_) => break,
                ClientOutcome::Conflict(certificate) => {
                    info!(
                        "Conflict while closing chain {:?}: {}. Retrying...",
                        chain_client.chain_id(),
                        certificate.hash()
                    );
                }
                ClientOutcome::WaitForTimeout(timeout) => {
                    info!(
                        "Waiting for timeout while closing chain {:?}: {}",
                        chain_client.chain_id(),
                        timeout
                    );
                    linera_base::time::timer::sleep(
                        timeout.timestamp.duration_since(Timestamp::now()),
                    )
                    .await;
                }
            }
        }

        debug!(
            "Closed chain {:?} in {} ms",
            chain_client.chain_id(),
            start.elapsed().as_millis()
        );

        Ok(())
    }

    /// Returns the chains to benchmark, from the config file if given, otherwise from the wallet.
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

/// Builds a fungible token transfer operation for the given application.
pub fn fungible_transfer(
    application_id: ApplicationId,
    chain_id: ChainId,
    sender: AccountOwner,
    receiver: AccountOwner,
    amount: Amount,
) -> Operation {
    let target_account = Account {
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

#[cfg(test)]
mod tests {
    use linera_base::{crypto::CryptoHash, identifiers::ChainId};

    use super::*;

    fn chain(seed: &str) -> ChainId {
        ChainId(CryptoHash::test_hash(seed))
    }

    /// `avoid_self` is what makes a mixed self/cross-chain workload expressible, and both
    /// generators must honour it: the CLI hands them the same interleaved destination list,
    /// so one ignoring the flag would silently measure 100% cross-chain traffic.
    #[test]
    fn avoid_self_decides_whether_the_source_is_a_destination() {
        let source = chain("source");
        let other = chain("other");
        // As the CLI builds it for --mixed-self-transfers: one self entry per cross entry.
        let interleaved = vec![other, source];

        for avoid_self in [true, false] {
            let mut native = NativeFungibleTransferGenerator::new(
                source,
                interleaved.clone(),
                false,
                avoid_self,
            )
            .unwrap();
            let mut fungible = FungibleTransferGenerator::new(
                ApplicationId::new(CryptoHash::test_hash("app")),
                source,
                interleaved.clone(),
                false,
                avoid_self,
            )
            .unwrap();

            let native_hits = (0..100)
                .filter(|_| native.next_destination() == source)
                .count();
            let fungible_hits = (0..100)
                .filter(|_| fungible.next_destination() == source)
                .count();

            if avoid_self {
                assert_eq!(native_hits, 0, "native sent to itself despite avoid_self");
                assert_eq!(
                    fungible_hits, 0,
                    "fungible sent to itself despite avoid_self"
                );
            } else {
                // The list is shuffled, so this is a ratio and not an alternation.
                assert!(
                    (30..=70).contains(&native_hits),
                    "native self-share {native_hits}/100 is not ~half"
                );
                assert!(
                    (30..=70).contains(&fungible_hits),
                    "fungible self-share {fungible_hits}/100 is not ~half"
                );
            }
        }
    }

    /// A lone destination is kept even when it is the source, or the generator would recurse
    /// forever looking for somewhere else to send.
    #[test]
    fn a_sole_self_destination_survives_avoid_self() {
        let source = chain("source");
        let mut generator =
            NativeFungibleTransferGenerator::new(source, vec![], false, true).unwrap();
        assert_eq!(generator.next_destination(), source);
    }
}
