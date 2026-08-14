// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pushing executed blocks to the other validators in the committee.
//!
//! One queue task per server process. Chain workers hand it every block they execute —
//! certificate and blobs, both already in memory — and it pushes them to the rest of the
//! committee. This is the dissemination that makes each validator a complete replica; consensus
//! alone only guarantees that a quorum holds any given block.
//!
//! One task per *process* rather than per chain worker, for two reasons. Nothing on this path may
//! touch a chain worker — a task that does resets the worker's keep-alive clock and keeps it
//! resident forever — so everything here reads storage only. And destination state must be
//! shared: with per-chain tasks, one unreachable validator is discovered, retried, and backed off
//! independently by every chain, which at scale is thousands of connection attempts against a
//! peer that may already be struggling. Here each destination has one connection, one backoff,
//! and one AIMD window that all chains share: a success widens the window additively, a
//! transport failure halves it, so the total in-flight load on a struggling peer shrinks
//! exponentially while it struggles and recovers once it stops.
//!
//! Failures are scoped by what they say. A timeout or transport error is about the
//! *destination*, so it halves the window and backs the destination off (and re-resolves its
//! node, which rotates to the next proxy when the transport is relayed). An error like
//! `EventsNotFound` is about one *chain* — the destination lacks the committee that signed the
//! certificate — so it backs off only that chain-destination pair: the admin chain's own export
//! is what will fix it, and must not be throttled by it.
//!
//! The queue is bounded, and a full queue drops the block rather than blocking the worker.
//! Dropping is safe because it is *repaired*: the queue tracks each chain's tip against what
//! every destination acknowledged, and closes any gap from storage during idle rounds. Per-chain
//! height ordering is preserved by allowing at most one in-flight send per chain-destination
//! pair.
//!
//! Chains converge and are forgotten: a record exists only while some destination is behind, so
//! memory is bounded by lagging chains, not by every chain the process ever served. Note what
//! that means during an outage — a destination that is down keeps every chain that produced a
//! block while it was gone, because that set *is* the work-list its catch-up needs. That is
//! bounded by the chains that were active, and [`metrics::TRACKED_CHAINS`] exposes it. The
//! work-list therefore covers chains seen since the process started — a validator that needs the
//! full history of a chain that never produces blocks again is out of scope here.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    iter,
    sync::{Arc, Mutex},
};

use futures::{stream::FuturesUnordered, FutureExt as _, StreamExt as _};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlockHeight, Epoch},
    identifiers::{BlobId, ChainId, StreamId},
    time::{timer::timeout, Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::{committee::Committee, system::EPOCH_STREAM_NAME};
use linera_storage::{Arc as CacheArc, Storage};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use crate::{
    client::chain_client,
    data_types::ChainInfoQuery,
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode, ValidatorNodeProvider},
    remote_node::RemoteNode,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, exponential_bucket_latencies, register_histogram,
        register_histogram_vec, register_int_counter, register_int_gauge, register_int_gauge_vec,
    };
    use prometheus::{Histogram, HistogramVec, IntCounter, IntGauge, IntGaugeVec};

    /// Chains the queue is tracking: those with a destination still behind, plus recently
    /// converged ones inside the retention window. A destination that is down holds every chain
    /// that produced a block during the outage here — that is the work-list its catch-up needs,
    /// and this is how an operator sees it growing.
    pub static TRACKED_CHAINS: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "block_export_tracked_chains",
            "Chains the export queue is tracking for catch-up",
        )
    });

    /// Blocks waiting in the export queue.
    pub static QUEUE_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "block_export_queue_size",
            "Blocks queued for export in this process",
        )
    });

    /// Blob payload bytes held by queued blocks, since a block count alone hides the memory a
    /// backlog of large blobs pins.
    pub static QUEUE_BYTES: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "block_export_queue_bytes",
            "Blob bytes held by blocks queued for export in this process",
        )
    });

    /// Blocks dropped because the queue was full. Each is repaired by a later catch-up round, so
    /// this counting up means latency, not loss.
    pub static DROPPED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
        register_int_counter(
            "block_export_dropped_blocks",
            "Blocks dropped from a full export queue, to be re-sent from storage",
        )
    });

    /// Time from a block being queued to its sends being scheduled.
    pub static EXPORT_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "block_export_latency",
            "Time (ms) a block waits in the export queue before its sends are scheduled",
            exponential_bucket_latencies(60_000.0),
        )
    });

    /// Time for one push to one destination, including any catch-up it triggered.
    pub static SEND_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "block_export_send_latency",
            "Time (ms) to push one block to one destination validator",
            &["validator"],
            exponential_bucket_latencies(60_000.0),
        )
    });

    /// How many concurrent sends each destination is currently allowed.
    pub static DESTINATION_WINDOW: LazyLock<IntGaugeVec> = LazyLock::new(|| {
        register_int_gauge_vec(
            "block_export_destination_window",
            "AIMD in-flight window per destination validator",
            &["validator"],
        )
    });

    /// How many blocks the destination was behind when we pushed to it: zero whenever the block
    /// was contiguous there, and the size of the gap we had to fill otherwise.
    pub static DESTINATION_LAG: LazyLock<HistogramVec> = LazyLock::new(|| {
        register_histogram_vec(
            "block_export_destination_lag",
            "Blocks a destination validator was missing when a block was pushed to it",
            &["validator"],
            exponential_bucket_interval(1.0, 100_000.0),
        )
    });
}

/// Configuration for pushing executed blocks to the other committee validators.
#[derive(Clone, Debug)]
pub struct BlockExportConfig {
    /// How many certificates are read from storage per catch-up read. Smaller than the
    /// client's 500: a chunk lives inside one send job, and `max_catch_up_blocks` bounds the
    /// round anyway.
    pub certificate_upload_batch_size: u64,
    /// How many blocks the export queue holds before dropping new ones for catch-up to repair.
    pub queue_size: usize,
    /// The most blob payload bytes queued blocks may pin before new ones are dropped for
    /// catch-up to repair — a block count alone lets 1024 blob-heavy blocks pin gigabytes.
    pub queue_bytes: usize,
    /// The most concurrent sends one destination is ever allowed — the AIMD window's ceiling.
    pub max_in_flight_per_destination: usize,
    /// How long a destination is skipped after a failed push, doubling up to `max_retry_delay`.
    /// Coarser than the transport's per-request retries: those decide whether one call is worth
    /// repeating, this decides whether the destination is worth attempting at all right now.
    pub retry_delay: Duration,
    /// The longest a failing destination is skipped for.
    pub max_retry_delay: Duration,
    /// How long the queue waits for a new block before spending a round catching up destinations
    /// that are behind. With `max_catch_up_blocks` this sets the backfill rate, so tune them
    /// together.
    pub idle_catch_up_interval: Duration,
    /// How many missing blocks are pushed to one destination per round. Deliberately small: it
    /// bounds what a live block may wait behind, and a validator that just joined reports height
    /// 0, so its catch-up is otherwise arbitrarily large.
    pub max_catch_up_blocks: u64,
    /// How long a converged chain's record is kept before being forgotten. Long enough for the
    /// chain's worker to fold the final heights into `exported_heights` on its next save; after
    /// it, a lost cursor costs one query to rebuild.
    pub converged_chain_retention: Duration,
}

impl BlockExportConfig {
    /// Rejects values that would make export misbehave rather than merely perform badly. Checked
    /// at startup, so a typo fails fast instead of surfacing as a panic, a spinning task, or gaps
    /// that are never repaired.
    pub fn check(&self) -> Result<(), String> {
        if self.certificate_upload_batch_size == 0 {
            // `slice::chunks(0)` panics.
            return Err("block export batch size must be greater than zero".into());
        }
        if self.queue_size == 0 {
            // Every block would be dropped and export would run on catch-up alone.
            return Err("block export queue size must be greater than zero".into());
        }
        if self.queue_bytes == 0 {
            // Every block carrying any blob would be dropped.
            return Err("block export queue byte budget must be greater than zero".into());
        }
        if self.max_in_flight_per_destination == 0 {
            // No destination could ever be sent anything.
            return Err("block export in-flight ceiling must be greater than zero".into());
        }
        if self.max_catch_up_blocks == 0 {
            // Every gap would stay open forever, silently.
            return Err("block export catch-up bound must be greater than zero".into());
        }
        if self.idle_catch_up_interval.is_zero() {
            // The idle timer would fire continuously, turning the task into a busy loop.
            return Err("block export idle interval must be greater than zero".into());
        }
        if self.retry_delay.is_zero() {
            // A failing destination would be retried without pause.
            return Err("block export retry delay must be greater than zero".into());
        }
        if self.max_retry_delay.is_zero() {
            // The cap would clamp every computed backoff to zero, same busy retry as above.
            return Err("block export max retry delay must be greater than zero".into());
        }
        if self.retry_delay > self.max_retry_delay {
            // The very first backoff would already exceed its own ceiling.
            return Err("block export retry delay must not exceed the max retry delay".into());
        }
        if self.converged_chain_retention.is_zero() {
            // The progress of a converged chain would be dropped before its worker folds it in.
            return Err("block export converged-chain retention must be greater than zero".into());
        }
        Ok(())
    }
}

impl Default for BlockExportConfig {
    fn default() -> Self {
        BlockExportConfig {
            certificate_upload_batch_size: 100,
            queue_size: 1024,
            queue_bytes: 256 * 1024 * 1024,
            max_in_flight_per_destination: 8,
            retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            idle_catch_up_interval: Duration::from_millis(200),
            max_catch_up_blocks: 200,
            converged_chain_retention: Duration::from_secs(300),
        }
    }
}

/// A block a chain worker has executed, on its way to the other validators.
struct ExportedBlock {
    certificate: CacheArc<ConfirmedBlockCertificate>,
    /// The block's required blobs, so that a destination missing them — which is always the case
    /// for a blob this very block publishes — is served without a read from storage. Held as the
    /// storage cache's pointers, so queued blocks share the allocations rather than copying them.
    blobs: Vec<CacheArc<Blob>>,
    /// The chain's epoch *after* the block was applied — a hint that lets the queue load a newer
    /// committee from storage, so a validator joining in this block is exported to immediately.
    epoch: Epoch,
    /// The persisted `exported_heights` of the chain, seeding missing cursors so a restart
    /// re-sends at most one block per destination instead of a history.
    exported_heights: BTreeMap<ValidatorPublicKey, BlockHeight>,
    /// Blob payload bytes, counted at enqueue so the dequeue releases the same amount of the
    /// byte budget.
    blob_bytes: usize,
    #[cfg(with_metrics)]
    queued_at: Instant,
}

/// The chain workers' end of the export queue: hands blocks over and reads back progress.
pub struct BlockExportHandle {
    blocks: mpsc::Sender<ExportedBlock>,
    progress: SharedProgress,
    tips: SharedTips,
    /// Blob payload bytes currently pinned by queued blocks, enforced against
    /// [`BlockExportConfig::queue_bytes`].
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
    queue_bytes_budget: usize,
}

impl Clone for BlockExportHandle {
    fn clone(&self) -> Self {
        BlockExportHandle {
            blocks: self.blocks.clone(),
            progress: self.progress.clone(),
            tips: self.tips.clone(),
            queued_bytes: self.queued_bytes.clone(),
            queue_bytes_budget: self.queue_bytes_budget,
        }
    }
}

/// The highest height each destination has acknowledged, per chain, written by the queue task
/// and folded into each chain's `exported_heights` by its worker on save. Entries are removed
/// when a chain converges, so this holds lagging chains only.
type SharedProgress = Arc<Mutex<HashMap<ChainId, BTreeMap<ValidatorPublicKey, BlockHeight>>>>;

/// The height after each chain's newest announced block. Written on every `export` call before
/// the queue is tried, so a block the full queue drops still raises the repair target the tick
/// measures destinations against.
type SharedTips = Arc<Mutex<HashMap<ChainId, BlockHeight>>>;

impl BlockExportHandle {
    /// Queues a block for export and returns immediately. A full queue drops the block — never
    /// blocks the worker — and the tip announced below is what lets catch-up re-send it from
    /// storage. `exported_heights` seeds the chain's cursors on its first block this process.
    pub(crate) fn export(
        &self,
        certificate: CacheArc<ConfirmedBlockCertificate>,
        blobs: Vec<CacheArc<Blob>>,
        epoch: Epoch,
        exported_heights: BTreeMap<ValidatorPublicKey, BlockHeight>,
    ) {
        // Announced before the queue is tried: a dropped block must still raise the repair
        // target, or a chain whose *last* block was dropped would never be repaired at all.
        {
            let header = &certificate.block().header;
            let tip = header.height.try_add_one().unwrap_or(BlockHeight::MAX);
            let mut tips = self.tips.lock().expect("tips mutex is never poisoned");
            let entry = tips.entry(header.chain_id).or_insert(tip);
            *entry = (*entry).max(tip);
        }
        let blob_bytes = blobs.iter().map(|blob| blob.bytes().len()).sum::<usize>();
        // The byte budget is enforced, not merely measured: a block count alone would let a few
        // blob-heavy blocks pin memory far past the cache's own bounds. Reserved *before* the
        // send: incrementing after `try_send` would let the queue task's decrement run first and
        // wrap the counter, spuriously exhausting the budget for every concurrent caller.
        let prior = self
            .queued_bytes
            .fetch_add(blob_bytes, std::sync::atomic::Ordering::Relaxed);
        if prior.saturating_add(blob_bytes) > self.queue_bytes_budget {
            self.queued_bytes
                .fetch_sub(blob_bytes, std::sync::atomic::Ordering::Relaxed);
            debug!(
                chain_id = %certificate.block().header.chain_id,
                height = %certificate.block().header.height,
                queued = prior, blob_bytes,
                "Export queue byte budget exhausted; dropping the block for catch-up to re-send",
            );
            #[cfg(with_metrics)]
            metrics::DROPPED_BLOCKS.inc();
            return;
        }
        let block = ExportedBlock {
            certificate,
            blobs,
            epoch,
            exported_heights,
            blob_bytes,
            #[cfg(with_metrics)]
            queued_at: Instant::now(),
        };
        match self.blocks.try_send(block) {
            Ok(()) => {
                #[cfg(with_metrics)]
                {
                    metrics::QUEUE_SIZE.inc();
                    metrics::QUEUE_BYTES.add(blob_bytes as i64);
                }
            }
            Err(mpsc::error::TrySendError::Full(block)) => {
                self.queued_bytes
                    .fetch_sub(blob_bytes, std::sync::atomic::Ordering::Relaxed);
                debug!(
                    chain_id = %block.certificate.block().header.chain_id,
                    height = %block.certificate.block().header.height,
                    "Export queue full; dropping the block for catch-up to re-send",
                );
                #[cfg(with_metrics)]
                metrics::DROPPED_BLOCKS.inc();
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.queued_bytes
                    .fetch_sub(blob_bytes, std::sync::atomic::Ordering::Relaxed);
                warn!("Block export queue stopped unexpectedly; blocks are no longer exported");
            }
        }
    }

    /// Returns how far each validator has been exported to on `chain_id`, restricted to
    /// `committee` so that validators which left it are pruned.
    pub(crate) fn progress(
        &self,
        chain_id: ChainId,
        committee: &Committee,
    ) -> BTreeMap<ValidatorPublicKey, BlockHeight> {
        let progress = self
            .progress
            .lock()
            .expect("progress mutex is never poisoned");
        let Some(chain_progress) = progress.get(&chain_id) else {
            return BTreeMap::new();
        };
        chain_progress
            .iter()
            .filter(|(validator, _)| committee.validators().contains_key(*validator))
            .map(|(validator, height)| (*validator, *height))
            .collect()
    }
}

/// Spawns the process-wide export queue task and returns the handle chain workers push to.
///
/// The task runs until every clone of the returned handle is dropped, and reads only from
/// `storage` — never through a chain worker, whose TTL a touch would reset.
pub fn spawn_block_export_queue<S, P>(
    storage: S,
    node_provider: Arc<P>,
    config: BlockExportConfig,
    own_public_key: Option<ValidatorPublicKey>,
) -> BlockExportHandle
where
    S: Storage + Clone + Send + Sync + 'static,
    P: ValidatorNodeProvider + Send + Sync + 'static,
    P::Node: Send + Sync,
{
    // Enforced here rather than only at the CLI: every constructor, tests included, must go
    // through it, and an invalid config panics at startup instead of mid-export.
    if let Err(message) = config.check() {
        panic!("invalid block export configuration: {message}");
    }
    let (blocks, receiver) = mpsc::channel(config.queue_size);
    let progress: SharedProgress = Arc::default();
    let tips: SharedTips = Arc::default();
    let queued_bytes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let queue_bytes_budget = config.queue_bytes;

    let task = BlockExportQueue {
        storage,
        node_provider,
        config,
        own_public_key,
        latest_epoch: None,
        committee: None,
        committee_dirty: false,
        admin_chain_id: None,
        ticks_until_scan: 0,
        chains: HashMap::new(),
        destinations: HashMap::new(),
        next_generation: 0,
        announced_epoch: None,
        scan_attempted_for: None,
        destinations_changed: false,
        queued_bytes: queued_bytes.clone(),
        progress: progress.clone(),
        tips: tips.clone(),
        draining: false,
    };
    linera_base::Task::spawn(task.run(receiver)).forget();

    BlockExportHandle {
        blocks,
        progress,
        tips,
        queued_bytes,
        queue_bytes_budget,
    }
}

/// What we know of one chain: its tip, and each destination's position below it. Kept while some
/// destination is behind, and for a grace window after convergence so the chain's worker can
/// still fold the final heights into its state; then forgotten.
struct ChainRecord {
    /// The height after the chain's last known block: what a destination must reach to be
    /// caught up.
    tip: BlockHeight,
    /// When this chain last saw a block or a completed send, for the convergence sweep.
    last_activity: Instant,
    dests: BTreeMap<ValidatorPublicKey, ChainDest>,
}

impl ChainRecord {
    /// Starts a record with a cursor for every current destination.
    ///
    /// Seeding here rather than at the call sites is what makes the empty-cursor state
    /// unrepresentable: a record with no cursors is invisible to the requeue loop and trivially
    /// "converged" to the sweep, so such a chain would be abandoned in silence. One creation
    /// path used to miss the seeding, and nothing in the suite could see it.
    fn new<N>(
        now: Instant,
        destinations: &HashMap<ValidatorPublicKey, DestState<N>>,
        exported_heights: &BTreeMap<ValidatorPublicKey, BlockHeight>,
    ) -> Self {
        ChainRecord {
            tip: BlockHeight::ZERO,
            last_activity: now,
            dests: destinations
                .keys()
                .map(|validator| {
                    // Seeded here rather than by a later fill: a cursor pre-populated as `None`
                    // and then only `or_insert`-ed would silently swallow the persisted height,
                    // leaving `exported_heights` write-only and costing a query per destination
                    // on every restart.
                    let next_height = exported_heights
                        .get(validator)
                        .and_then(|height| height.try_add_one().ok());
                    (
                        *validator,
                        ChainDest {
                            next_height,
                            ..ChainDest::default()
                        },
                    )
                })
                .collect(),
        }
    }
}

impl ChainRecord {
    /// Fills in every cursor this record does not have from the chain's persisted heights.
    ///
    /// Applied on every block rather than at record creation, and idempotent. Tying the seeding
    /// to creation has failed twice — the tick creates records too, from a tip announced before
    /// its block was dequeued, and it has no heights to seed with; an `or_insert` then cannot
    /// correct a cursor that already exists as `None`.
    ///
    /// Safe for a cursor a failure cleared, too: the persisted height is a lower bound, so the
    /// worst case is re-offering blocks the destination already holds, and its reply corrects
    /// the cursor in either direction.
    fn seed_missing_cursors<N>(
        &mut self,
        destinations: &HashMap<ValidatorPublicKey, DestState<N>>,
        exported_heights: &BTreeMap<ValidatorPublicKey, BlockHeight>,
    ) {
        for validator in destinations.keys() {
            let seed = exported_heights
                .get(validator)
                .and_then(|height| height.try_add_one().ok());
            let chain_dest = self.dests.entry(*validator).or_default();
            if chain_dest.next_height.is_none() && chain_dest.in_flight.is_none() {
                chain_dest.next_height = seed;
            }
        }
    }
}

/// One chain's cursor at one destination.
#[derive(Default)]
struct ChainDest {
    /// The next height the destination needs, or `None` when we have to ask it — before the
    /// first push, and after any failed one.
    next_height: Option<BlockHeight>,
    /// The destination generation of the send currently running for this pair, if any. Carrying
    /// the generation is what stops a stale job's completion from clearing a *newer* job's flag
    /// and breaking the one-send-per-pair ordering invariant.
    in_flight: Option<u64>,
    /// Whether this chain already sits in the destination's ready list, to keep it there once.
    queued: bool,
    /// Backoff for *chain-scoped* failures — the destination is healthy but cannot accept this
    /// chain yet, e.g. it lacks the committee and the admin chain's export has not reached it.
    retry_at: Option<Instant>,
    failures: u32,
}

/// One destination validator: its connection and the health state every chain shares.
struct DestState<N> {
    node: N,
    address: String,
    /// Bumped whenever this state is rebuilt, so a send started against a previous incarnation
    /// cannot corrupt the new one's accounting when it completes.
    generation: u64,
    /// Sends currently running against this destination, over all chains.
    in_flight: usize,
    /// How many concurrent sends the AIMD control currently allows: +1 per success up to the
    /// configured ceiling, halved per transport failure down to 1.
    window: usize,
    /// Backoff for *destination-scoped* failures: transport errors and timeouts.
    retry_at: Option<Instant>,
    failures: u32,
    /// Chains with work for this destination, drained as the window allows.
    ready: VecDeque<ChainId>,
}

/// How one send ended, scoped to what the error tells us about.
enum SendOutcome {
    /// The validator answered; its reported next height.
    Reached(BlockHeight),
    /// The failure is about this chain on this destination, not about the destination.
    ChainScoped(Box<chain_client::Error>),
    /// The failure is about the destination itself.
    DestinationScoped(Box<chain_client::Error>),
}

/// The body of the process-wide export queue task.
struct BlockExportQueue<S, P>
where
    S: Storage,
    P: ValidatorNodeProvider,
{
    storage: S,
    node_provider: Arc<P>,
    config: BlockExportConfig,
    own_public_key: Option<ValidatorPublicKey>,
    /// The newest committee seen, from exported blocks and from scanning storage forward. Used
    /// for the destination set of every chain: a chain's own committee cannot announce a
    /// newcomer, and current committee members need every chain regardless of its epoch.
    latest_epoch: Option<Epoch>,
    committee: Option<Arc<Committee>>,
    /// Set when `committee` changed and the destination set has not been rebuilt yet, so the
    /// rebuild runs per committee change rather than per block.
    committee_dirty: bool,
    /// The admin chain, read from the network description once, for the silent epoch probe.
    admin_chain_id: Option<ChainId>,
    /// Ticks until the next storage scan for a committee no block has carried yet.
    ticks_until_scan: u32,
    chains: HashMap<ChainId, ChainRecord>,
    destinations: HashMap<ValidatorPublicKey, DestState<P::Node>>,
    /// The last destination generation handed out; never reused within this queue's lifetime.
    next_generation: u64,
    /// The newest epoch any exported block has announced; the tick loads its committee when it
    /// is ahead of `latest_epoch`.
    announced_epoch: Option<Epoch>,
    /// The announcement the eager scan last acted on, so one that fails to load is retried on
    /// the normal cadence rather than on every tick.
    scan_attempted_for: Option<Epoch>,
    /// Set when `sync_destinations` changed the set, so the per-record cursor fill runs once
    /// per change instead of once per tick.
    destinations_changed: bool,
    /// Blob bytes pinned by queued blocks, shared with every handle for budget enforcement.
    queued_bytes: Arc<std::sync::atomic::AtomicUsize>,
    progress: SharedProgress,
    /// The highest height each chain has announced, written by every `export` call — including
    /// ones the full queue dropped — so a dropped block still moves the repair target.
    tips: SharedTips,
    /// True once every handle is dropped: completions may finish, nothing new starts.
    draining: bool,
}

/// How many ticks apart the queue probes storage for committees no block has announced.
const TICKS_PER_COMMITTEE_SCAN: u32 = 10;

impl<S, P> BlockExportQueue<S, P>
where
    S: Storage + Clone + Send + Sync + 'static,
    P: ValidatorNodeProvider,
    P::Node: Clone + Send + 'static,
{
    /// Exports blocks until every handle is dropped, ticking every `idle_catch_up_interval`
    /// whether or not sends are in flight — backoff expiry and gap repair must not wait for a
    /// process-wide lull that a busy validator never has.
    #[instrument(level = "debug", skip_all)]
    async fn run(mut self, mut receiver: mpsc::Receiver<ExportedBlock>) {
        /// What woke the loop, decided inside the select so its borrows end before handling.
        enum Wake {
            Done(JobDone),
            Block(Option<ExportedBlock>),
            Tick,
        }
        let mut jobs = FuturesUnordered::new();
        // One deadline carried across iterations: a timer rebuilt per iteration restarts on
        // every completion or block, so under sustained load it would never fire — and with it
        // would die every tick-only duty (drop repair, backoff expiry, the committee scan, the
        // convergence sweep).
        let interval = self.config.idle_catch_up_interval;
        let mut next_tick = Instant::now() + interval;
        loop {
            let now = Instant::now();
            if now >= next_tick {
                self.tick(&mut jobs).await;
                next_tick = Instant::now() + interval;
                continue;
            }
            let until_tick = next_tick.duration_since(now);
            let wake = if jobs.is_empty() {
                match timeout(until_tick, receiver.recv()).await {
                    Ok(received) => Wake::Block(received),
                    Err(_) => Wake::Tick,
                }
            } else {
                // `futures::select_biased` rather than `tokio::select`, which does not compile
                // for the web target. Completions first, then fresh blocks, then the tick.
                futures::select_biased! {
                    done = jobs.next() => Wake::Done(done.expect("jobs is not empty")),
                    received = receiver.recv().fuse() => Wake::Block(received),
                    _ = linera_base::time::timer::sleep(until_tick).fuse() => Wake::Tick,
                }
            };
            match wake {
                Wake::Done(done) => self.on_done(done, &mut jobs),
                Wake::Block(Some(block)) => self.on_block(block, &mut jobs),
                Wake::Block(None) => break,
                Wake::Tick => {
                    self.tick(&mut jobs).await;
                    next_tick = Instant::now() + interval;
                }
            }
        }
        // The workers are gone. Let in-flight sends finish, without starting new ones — every
        // completion would otherwise drain more catch-up work and hold shutdown open.
        self.draining = true;
        while let Some(done) = jobs.next().await {
            self.on_done(done, &mut jobs);
        }
        debug!("All block export handles dropped; stopping the export queue");
    }

    /// Folds a fresh block in: advances the chain's tip and fans out to every destination that
    /// can take it now; the rest catch up from storage when their turn comes.
    fn on_block(&mut self, block: ExportedBlock, jobs: &mut FuturesUnordered<JobFuture>) {
        self.queued_bytes
            .fetch_sub(block.blob_bytes, std::sync::atomic::Ordering::Relaxed);
        #[cfg(with_metrics)]
        {
            metrics::QUEUE_SIZE.dec();
            metrics::QUEUE_BYTES.sub(block.blob_bytes as i64);
            metrics::EXPORT_LATENCY
                .finish_measurement(block.queued_at.elapsed().as_secs_f64() * 1000.0);
        }
        let header = &block.certificate.block().header;
        let (chain_id, height) = (header.chain_id, header.height);
        // Only recorded here: loading the committee reads storage, and an await in this handler
        // stalls every in-flight send — the tick's scan does the loading.
        if self
            .announced_epoch
            .is_none_or(|announced| block.epoch > announced)
        {
            self.announced_epoch = Some(block.epoch);
        }
        // Per committee change only. Never on "destinations are empty": when a committee's
        // addresses cannot be resolved that stays true, and the rebuild walks every tracked
        // chain and re-warns per member — on the block-execution path. The tick retries it.
        if self.committee_dirty {
            self.sync_destinations();
        }

        let now = Instant::now();
        let tip = height.try_add_one().unwrap_or(BlockHeight::MAX);
        let record = self
            .chains
            .entry(chain_id)
            .or_insert_with(|| ChainRecord::new(now, &self.destinations, &block.exported_heights));
        record.tip = record.tip.max(tip);
        record.last_activity = now;
        record.seed_missing_cursors(&self.destinations, &block.exported_heights);
        let validators = self.destinations.keys().copied().collect::<Vec<_>>();
        for validator in validators {
            let record = self.chains.get_mut(&chain_id).expect("inserted above");
            let chain_dest = record.dests.entry(validator).or_default();
            let dest = self
                .destinations
                .get_mut(&validator)
                .expect("iterating destinations");
            let contiguous = chain_dest.next_height == Some(height);
            let can_send_now = chain_dest.in_flight.is_none()
                && chain_dest.retry_at.is_none_or(|at| at <= now)
                && dest.retry_at.is_none_or(|at| at <= now)
                && dest.in_flight < dest.window;
            if contiguous && can_send_now {
                // The fast path: the block is already in memory and the destination is ready,
                // so no storage read at all.
                Self::spawn_job(
                    jobs,
                    &self.storage,
                    &self.config,
                    chain_id,
                    validator,
                    dest,
                    chain_dest,
                    record.tip,
                    Some((block.certificate.clone(), block.blobs.clone())),
                );
            } else if !chain_dest.queued {
                chain_dest.queued = true;
                dest.ready.push_back(chain_id);
                if can_send_now {
                    Self::drain_ready(
                        &mut self.chains,
                        &self.storage,
                        &self.config,
                        validator,
                        dest,
                        jobs,
                        now,
                    );
                }
            }
        }
    }

    /// Folds one finished send back into the destination's and the chain's state.
    fn on_done(
        &mut self,
        (chain_id, validator, generation, outcome): JobDone,
        jobs: &mut FuturesUnordered<JobFuture>,
    ) {
        let now = Instant::now();
        let Some(dest) = self.destinations.get_mut(&validator) else {
            return; // The validator left the committee while its send was in flight.
        };
        if dest.generation != generation {
            // The send ran against a previous incarnation of this destination; its slot was
            // never counted here and its result must not touch the fresh state.
            if let Some(chain_dest) = self
                .chains
                .get_mut(&chain_id)
                .and_then(|record| record.dests.get_mut(&validator))
            {
                // Only the flag this very job set — the pair may since carry a newer job's.
                if chain_dest.in_flight == Some(generation) {
                    chain_dest.in_flight = None;
                }
            }
            return;
        }
        dest.in_flight = dest.in_flight.saturating_sub(1);

        // Destination health comes from the outcome alone, never gated on the chain record —
        // a transport failure must shrink the window and set the backoff even for a chain the
        // queue has since forgotten.
        match &outcome {
            SendOutcome::Reached(_) => {
                dest.failures = 0;
                dest.retry_at = None;
                dest.window = (dest.window + 1).min(self.config.max_in_flight_per_destination);
            }
            SendOutcome::ChainScoped(_) => {}
            SendOutcome::DestinationScoped(error) => {
                warn!(
                    validator = %dest.address, %chain_id, %error,
                    "Failed to export to a validator; backing it off and re-resolving",
                );
                dest.window = (dest.window / 2).max(1);
                back_off(&mut dest.failures, &mut dest.retry_at, now, &self.config);
                // Re-resolve so a relayed transport draws the next proxy from the rotation; the
                // backoff above still applies if the validator itself is the problem. Resolved
                // through the list API because the test provider resolves by *key*, which only
                // the list variant carries.
                match self
                    .node_provider
                    .make_nodes_from_list(iter::once((validator, dest.address.clone())))
                {
                    Ok(mut nodes) => {
                        if let Some((_, node)) = nodes.next() {
                            dest.node = node;
                        }
                    }
                    Err(error) => {
                        warn!(%validator, %error, "Cannot re-resolve a failing destination");
                    }
                }
            }
        }
        #[cfg(with_metrics)]
        metrics::DESTINATION_WINDOW
            .with_label_values(&[&dest.address])
            .set(dest.window as i64);

        if let Some(record) = self.chains.get_mut(&chain_id) {
            record.last_activity = now;
            if let Some(chain_dest) = record.dests.get_mut(&validator) {
                if chain_dest.in_flight == Some(generation) {
                    chain_dest.in_flight = None;
                }
                match &outcome {
                    SendOutcome::Reached(next_height) => {
                        let previous = chain_dest.next_height;
                        let advanced = previous.is_none_or(|n| *next_height > n);
                        // The validator is authoritative about its own height, in both
                        // directions: only one send per pair is ever in flight and stale
                        // generations are filtered above, so this is not a race. One restored
                        // from a backup reports a *lower* height, and refusing to believe it
                        // would leave that gap unrepaired for good — we would think it caught
                        // up and never send the blocks it lost.
                        //
                        // Untested: the in-process harness has no way to make a validator lose
                        // state, so this direction rests on the argument above rather than on a
                        // failing-first test. A regression is noticed only once a send happens,
                        // which needs the chain's tip to be above the cursor. One that regresses while we
                        // believe it fully converged waits for the chain's next block; probing
                        // converged pairs instead would cost a query per chain per destination
                        // forever, to catch something only a restore causes.
                        chain_dest.next_height = Some(*next_height);
                        if advanced || previous.is_some_and(|n| *next_height < n) {
                            chain_dest.failures = 0;
                            chain_dest.retry_at = None;
                            if let Ok(acked) = next_height.try_sub_one() {
                                self.progress
                                    .lock()
                                    .expect("progress mutex is never poisoned")
                                    .entry(chain_id)
                                    .or_default()
                                    .insert(validator, acked);
                            }
                        } else if *next_height < record.tip {
                            // Answered but moved nothing — a gap our storage cannot fill — so
                            // back this pair off rather than spinning on it.
                            back_off(
                                &mut chain_dest.failures,
                                &mut chain_dest.retry_at,
                                now,
                                &self.config,
                            );
                        }
                    }
                    SendOutcome::ChainScoped(error) => {
                        debug!(
                            %chain_id, %validator, %error,
                            "Destination cannot accept this chain yet; backing the pair off",
                        );
                        chain_dest.next_height = None;
                        back_off(
                            &mut chain_dest.failures,
                            &mut chain_dest.retry_at,
                            now,
                            &self.config,
                        );
                    }
                    SendOutcome::DestinationScoped(_) => {
                        chain_dest.next_height = None;
                    }
                }
            }
        }

        if self.draining {
            return;
        }
        // A pair that advanced but is still behind continues on the next free slot rather than
        // waiting for a tick — multi-round catch-up must not depend on a process-wide lull.
        let dest = self
            .destinations
            .get_mut(&validator)
            .expect("checked above");
        if let Some(record) = self.chains.get_mut(&chain_id) {
            let tip = record.tip;
            if let Some(chain_dest) = record.dests.get_mut(&validator) {
                let behind = chain_dest.next_height.is_none_or(|next| next < tip);
                if behind && !chain_dest.queued && chain_dest.in_flight.is_none() {
                    chain_dest.queued = true;
                    dest.ready.push_back(chain_id);
                }
            }
        }
        Self::drain_ready(
            &mut self.chains,
            &self.storage,
            &self.config,
            validator,
            dest,
            jobs,
            now,
        );
    }

    /// An idle moment: pick up committee changes from storage and requeue expired backoffs.
    async fn tick(&mut self, jobs: &mut FuturesUnordered<JobFuture>) {
        let now = Instant::now();
        // Occasionally scan storage for committees no block has carried — how a validator
        // admitted while every chain is idle still becomes a destination. On its own cadence
        // because the probe reads storage, and ticks now fire even under load.
        // Eagerly, but at most once per announced epoch: a committee that cannot be loaded
        // leaves `latest_epoch` behind, and re-triggering on the same announcement would turn
        // the throttled scan into a storage read every tick for as long as it stays unloadable.
        // The cadence below still retries it.
        let announced_newer = self.announced_epoch.is_some_and(|epoch| {
            self.latest_epoch.is_none_or(|latest| epoch > latest)
                && self.scan_attempted_for != Some(epoch)
        });
        if self.ticks_until_scan == 0 || announced_newer {
            self.ticks_until_scan = TICKS_PER_COMMITTEE_SCAN;
            self.scan_attempted_for = self.announced_epoch;
            self.scan_committees().await;
        } else {
            self.ticks_until_scan -= 1;
        }
        if self.committee_dirty || (self.destinations.is_empty() && self.committee.is_some()) {
            self.sync_destinations();
        }

        // Drain announced tips in: this is what repairs a block the full queue dropped, and what
        // creates the record when even a chain's first block was dropped. Taken rather than
        // cloned — the workers contend on this mutex every block, and once folded the records
        // carry the truth.
        let tips = std::mem::take(&mut *self.tips.lock().expect("tips mutex is never poisoned"));
        for (chain_id, tip) in tips {
            let record = self
                .chains
                .entry(chain_id)
                .or_insert_with(|| ChainRecord::new(now, &self.destinations, &BTreeMap::new()));
            record.tip = record.tip.max(tip);
        }
        // When the destination set changed, every destination gets a cursor on every tracked
        // chain, so a validator that joined after a chain's last block is still caught up on it.
        // Gated on the change: this walks every record.
        if self.destinations_changed {
            self.destinations_changed = false;
            for record in self.chains.values_mut() {
                for validator in self.destinations.keys() {
                    record.dests.entry(*validator).or_default();
                }
            }
        }

        // Forget chains that converged and stayed quiet past the retention window, so memory
        // tracks recent and lagging chains rather than every chain ever seen. The grace window
        // is what lets the chain's worker fold the final heights in before they vanish.
        // Convergence is judged against the *current* destination set — an empty one (a
        // single-validator committee) is trivially converged, not immortal.
        let retention = self.config.converged_chain_retention;
        let destinations = &self.destinations;
        let mut forgotten = Vec::new();
        self.chains.retain(|chain_id, record| {
            let converged = destinations.keys().all(|validator| {
                record.dests.get(validator).is_some_and(|chain_dest| {
                    // `>=`: a destination is routinely *ahead* of our tip — the client
                    // broadcasts to everyone — and ahead must count as done, not as never
                    // converging.
                    chain_dest.in_flight.is_none()
                        && chain_dest
                            .next_height
                            .is_some_and(|next| next >= record.tip)
                })
            });
            if converged && now.duration_since(record.last_activity) > retention {
                forgotten.push(*chain_id);
                false
            } else {
                true
            }
        });
        if !forgotten.is_empty() {
            let mut progress = self
                .progress
                .lock()
                .expect("progress mutex is never poisoned");
            for chain_id in &forgotten {
                progress.remove(chain_id);
            }
        }

        #[cfg(with_metrics)]
        metrics::TRACKED_CHAINS.set(self.chains.len() as i64);

        // Requeue everything whose backoff expired and is still behind.
        for (chain_id, record) in &mut self.chains {
            for (validator, chain_dest) in &mut record.dests {
                let behind = chain_dest.next_height.is_none_or(|next| next < record.tip);
                if behind
                    && chain_dest.in_flight.is_none()
                    && !chain_dest.queued
                    && chain_dest.retry_at.is_none_or(|at| at <= now)
                {
                    if let Some(dest) = self.destinations.get_mut(validator) {
                        chain_dest.queued = true;
                        dest.ready.push_back(*chain_id);
                    }
                }
            }
        }
        let validators = self.destinations.keys().copied().collect::<Vec<_>>();
        for validator in validators {
            let dest = self
                .destinations
                .get_mut(&validator)
                .expect("iterating destinations");
            Self::drain_ready(
                &mut self.chains,
                &self.storage,
                &self.config,
                validator,
                dest,
                jobs,
                now,
            );
        }
    }

    /// Scans storage for committees newer than the one in use, by listing the admin chain's
    /// epoch events from the frontier — one bulk read, immune to holes in the history, and
    /// silent when nothing is new.
    async fn scan_committees(&mut self) {
        if self.admin_chain_id.is_none() {
            self.admin_chain_id = match self.storage.read_network_description().await {
                Ok(Some(description)) => Some(description.admin_chain_id),
                Ok(None) => return,
                Err(error) => {
                    debug!(%error, "Cannot read the network description to scan for committees");
                    return;
                }
            };
        }
        let Some(admin_chain_id) = self.admin_chain_id else {
            return;
        };
        let start = self
            .latest_epoch
            .map_or(0, |epoch| epoch.0.saturating_add(1));
        // Epoch 0 comes from the genesis blob, not an event, so it is never in the list.
        let genesis = (start == 0).then_some(Epoch(0));
        let mut candidates = match self
            .storage
            .read_events_from_index(&admin_chain_id, &StreamId::system(EPOCH_STREAM_NAME), start)
            .await
        {
            Ok(events) => events
                .into_iter()
                .map(|event| Epoch(event.index))
                .chain(genesis)
                .collect::<Vec<_>>(),
            Err(error) => {
                debug!(%error, "Cannot list epoch events to scan for committees");
                return;
            }
        };
        // Newest first, and stop at the first that loads: a committee lists the full validator
        // set, so the newest usable one subsumes the rest. Trying only the newest would stall
        // the whole scan whenever its blob happens to be missing while an older — but still
        // newer than ours — one is right there.
        candidates.sort_unstable_by(|a, b| b.cmp(a));
        for epoch in candidates {
            match self.storage.get_or_load_committee(epoch).await {
                Ok(Some(committee)) => {
                    self.latest_epoch = Some(epoch);
                    self.committee = Some(committee);
                    self.committee_dirty = true;
                    return;
                }
                Ok(None) => debug!(%epoch, "An epoch event exists but its committee cannot load"),
                Err(error) => debug!(%error, %epoch, "Cannot load a committee from storage"),
            }
        }
    }

    /// Brings the destination set in line with the latest committee: adds joiners, drops
    /// leavers, and re-resolves a changed address.
    fn sync_destinations(&mut self) {
        let Some(committee) = &self.committee else {
            return;
        };
        self.committee_dirty = false;
        let mut rebuilt = Vec::new();
        #[cfg(with_metrics)]
        let mut rebuilt_addresses = Vec::new();
        self.destinations.retain(|validator, dest| {
            let keep = committee
                .validators()
                .get(validator)
                .is_some_and(|state| state.network_address == dest.address);
            if !keep {
                rebuilt.push(*validator);
                #[cfg(with_metrics)]
                rebuilt_addresses.push(dest.address.clone());
            }
            keep
        });
        if !rebuilt.is_empty() {
            self.destinations_changed = true;
        }
        // Drop the metric series of every address that just went away, so a departed validator
        // does not leave a window gauge frozen at its last value and a lag histogram that never
        // moves again — both read as a live destination that simply stopped changing.
        #[cfg(with_metrics)]
        for address in &rebuilt_addresses {
            // A series that was never created is simply absent; nothing to report either way.
            metrics::DESTINATION_WINDOW
                .remove_label_values(&[address])
                .ok();
            metrics::SEND_LATENCY.remove_label_values(&[address]).ok();
            metrics::DESTINATION_LAG
                .remove_label_values(&[address])
                .ok();
        }
        // A dropped DestState takes its ready list with it, so clear the queued marks that
        // pointed into it — a queued pair is invisible to every requeue path.
        for record in self.chains.values_mut() {
            for validator in &rebuilt {
                if let Some(chain_dest) = record.dests.get_mut(validator) {
                    chain_dest.queued = false;
                }
            }
        }
        for (validator, address) in committee.validator_addresses() {
            if Some(validator) == self.own_public_key || self.destinations.contains_key(&validator)
            {
                continue;
            }
            // One at a time: a batch fails whole on the first bad address, and one unresolvable
            // validator must not keep every other one out of the destination set. Through the
            // list API rather than `make_node`, because the test provider resolves by *key*,
            // which only the list variant carries.
            match self
                .node_provider
                .make_nodes_from_list(iter::once((validator, address)))
            {
                Ok(mut nodes) => {
                    if let Some((_, node)) = nodes.next() {
                        // Monotonic across the queue's lifetime: derived from surviving
                        // destinations it could repeat after the set empties, and a repeat lets
                        // a stale in-flight send corrupt a fresh incarnation's accounting.
                        self.next_generation += 1;
                        self.destinations_changed = true;
                        self.destinations.insert(
                            validator,
                            DestState {
                                node,
                                address: address.to_owned(),
                                generation: self.next_generation,
                                in_flight: 0,
                                window: self.config.max_in_flight_per_destination,
                                retry_at: None,
                                failures: 0,
                                ready: VecDeque::new(),
                            },
                        );
                    }
                }
                Err(error) => {
                    warn!(
                        %validator, %address, %error,
                        "Cannot resolve a committee member to export blocks to; \
                         continuing with the others",
                    );
                }
            }
        }
        // A validator that left takes its cursors with it.
        self.chains.retain(|_, record| {
            record
                .dests
                .retain(|validator, _| self.destinations.contains_key(validator));
            true
        });
    }
}

/// The result of one send job: which pair it was for, under which destination generation, and
/// how it went.
type JobDone = (ChainId, ValidatorPublicKey, u64, SendOutcome);

/// One send in flight, boxed so jobs from different call sites share a queue.
#[cfg(not(web))]
type JobFuture = futures::future::BoxFuture<'static, JobDone>;
#[cfg(web)]
type JobFuture = futures::future::LocalBoxFuture<'static, JobDone>;

impl<S, P> BlockExportQueue<S, P>
where
    S: Storage + Clone + Send + Sync + 'static,
    P: ValidatorNodeProvider,
    P::Node: Clone + Send + 'static,
{
    /// Starts one send for `(chain_id, validator)`: the held block when contiguous, catch-up
    /// from storage otherwise.
    #[expect(clippy::too_many_arguments)]
    fn spawn_job(
        jobs: &mut FuturesUnordered<JobFuture>,
        storage: &S,
        config: &BlockExportConfig,
        chain_id: ChainId,
        validator: ValidatorPublicKey,
        dest: &mut DestState<P::Node>,
        chain_dest: &mut ChainDest,
        target: BlockHeight,
        live: Option<(CacheArc<ConfirmedBlockCertificate>, Vec<CacheArc<Blob>>)>,
    ) {
        let generation = dest.generation;
        chain_dest.in_flight = Some(generation);
        chain_dest.queued = false;
        dest.in_flight += 1;
        let mut sender = BlockSender {
            remote_node: RemoteNode {
                public_key: validator,
                node: dest.node.clone(),
            },
            storage: storage.clone(),
            certificate_upload_batch_size: config.certificate_upload_batch_size,
        };
        let cursor = chain_dest.next_height;
        let max_catch_up = config.max_catch_up_blocks;
        #[cfg(with_metrics)]
        let address = dest.address.clone();
        let job = async move {
            #[cfg(with_metrics)]
            let send_latency = metrics::SEND_LATENCY.with_label_values(&[&address]);
            #[cfg(with_metrics)]
            let _latency = send_latency.measure_latency();
            #[cfg(with_metrics)]
            metrics::DESTINATION_LAG
                .with_label_values(&[&address])
                .observe(match cursor {
                    Some(next) => target.0.saturating_sub(next.0) as f64,
                    None => 1.0,
                });
            let result = match live {
                Some((certificate, blobs)) => {
                    sender
                        .send_block(&certificate, &blobs, cursor, max_catch_up)
                        .await
                }
                None => {
                    sender
                        .send_missing_blocks(chain_id, target, cursor, max_catch_up)
                        .await
                }
            };
            let outcome = match result {
                Ok(next_height) => SendOutcome::Reached(next_height),
                Err(error) if is_chain_scoped(&error) => SendOutcome::ChainScoped(Box::new(error)),
                Err(error) => SendOutcome::DestinationScoped(Box::new(error)),
            };
            (chain_id, validator, generation, outcome)
        };
        #[cfg(not(web))]
        jobs.push(job.boxed());
        #[cfg(web)]
        jobs.push(job.boxed_local());
    }

    /// Starts catch-up sends from this destination's ready list until its window is full.
    fn drain_ready(
        chains: &mut HashMap<ChainId, ChainRecord>,
        storage: &S,
        config: &BlockExportConfig,
        validator: ValidatorPublicKey,
        dest: &mut DestState<P::Node>,
        jobs: &mut FuturesUnordered<JobFuture>,
        now: Instant,
    ) {
        if dest.retry_at.is_some_and(|at| at > now) {
            return;
        }
        while dest.in_flight < dest.window {
            let Some(chain_id) = dest.ready.pop_front() else {
                return;
            };
            let Some(record) = chains.get_mut(&chain_id) else {
                continue; // Converged and dropped while queued.
            };
            let Some(chain_dest) = record.dests.get_mut(&validator) else {
                continue;
            };
            chain_dest.queued = false;
            let behind = chain_dest.next_height.is_none_or(|next| next < record.tip);
            if chain_dest.in_flight.is_some()
                || !behind
                || chain_dest.retry_at.is_some_and(|at| at > now)
            {
                continue; // The tick that queued it has been overtaken; it requeues if needed.
            }
            Self::spawn_job(
                jobs, storage, config, chain_id, validator, dest, chain_dest, record.tip, None,
            );
        }
    }
}

/// Whether this failure is about one chain rather than about the destination. `EventsNotFound`
/// is the destination lacking a committee — the admin chain's export fixes that, and must not be
/// throttled by it; the others are gaps on our own side.
fn is_chain_scoped(error: &chain_client::Error) -> bool {
    matches!(
        error,
        chain_client::Error::RemoteNodeError(
            NodeError::EventsNotFound(_)
                | NodeError::BlobsNotFound(_)
                | NodeError::InactiveChain(_)
        ) | chain_client::Error::ReadCertificatesError(_)
            // Our own storage failing to read is nobody's health signal; halving the
            // destination's window for it would punish the wrong side.
            | chain_client::Error::ViewError(_)
    )
}

/// Escalating backoff shared by both scopes: doubles from `retry_delay` per consecutive failure,
/// capped at `max_retry_delay`.
fn back_off(
    failures: &mut u32,
    retry_at: &mut Option<Instant>,
    now: Instant,
    config: &BlockExportConfig,
) {
    let backoff = config
        .retry_delay
        .saturating_mul(1u32.checked_shl(*failures).unwrap_or(u32::MAX))
        .min(config.max_retry_delay);
    *retry_at = Some(now + backoff);
    *failures = failures.saturating_add(1);
}

/// Sends this validator's blocks to one other validator, reading everything it needs from
/// storage.
///
/// This is deliberately not [`crate::updater::RemoteNodeUpdater`]: that is the client's tool and
/// holds a local node, and anything on the export path that reaches a chain worker resets its
/// TTL. Blobs and certificates for committed blocks are always durable before export sees them
/// (`write_blobs_and_certificate` precedes execution), so storage is sufficient.
pub(crate) struct BlockSender<S, N> {
    pub(crate) remote_node: RemoteNode<N>,
    pub(crate) storage: S,
    pub(crate) certificate_upload_batch_size: u64,
}

impl<S, N> BlockSender<S, N>
where
    S: Storage + Clone + 'static,
    N: ValidatorNode + Clone + 'static,
{
    /// Pushes a block the caller already holds, first closing up to `max_catch_up` of any gap
    /// below it, and returns the height the validator reports afterwards.
    ///
    /// The held block is sent only once the gap is gone: one landing above a gap is silently
    /// preprocessed and never advances the tip.
    pub(crate) async fn send_block(
        &mut self,
        certificate: &CacheArc<ConfirmedBlockCertificate>,
        blobs: &[CacheArc<Blob>],
        destination_next_height: Option<BlockHeight>,
        max_catch_up: u64,
    ) -> Result<BlockHeight, chain_client::Error> {
        let block = certificate.block();
        let (chain_id, height) = (block.header.chain_id, block.header.height);

        let next_height = if destination_next_height == Some(height) {
            height
        } else {
            self.send_missing_blocks(chain_id, height, destination_next_height, max_catch_up)
                .await?
        };
        // Not exactly at this block: either the gap was larger than one chunk, or the validator
        // is already past it — a re-executed chain re-offers its whole history — and in both
        // cases sending would be waste, so report the truth instead.
        if next_height != height {
            return Ok(next_height);
        }
        let info = self.send_confirmed_certificate(certificate, blobs).await?;
        Ok(info.next_block_height)
    }

    /// Sends up to `max_blocks` of the blocks of `chain_id` the validator is missing below
    /// `target_next_height`, returning the height it reports afterwards.
    ///
    /// Bounded so a validator that just joined converges over rounds instead of blocking the
    /// caller once. Heights whose certificates are not in storage are skipped — a chain we merely
    /// receive from is stored only at its message-bearing blocks, and the destination
    /// preprocesses above such gaps.
    pub(crate) async fn send_missing_blocks(
        &mut self,
        chain_id: ChainId,
        target_next_height: BlockHeight,
        destination_next_height: Option<BlockHeight>,
        max_blocks: u64,
    ) -> Result<BlockHeight, chain_client::Error> {
        let mut next_height = match destination_next_height {
            Some(height) => height,
            None => {
                let query = ChainInfoQuery::new(chain_id);
                self.remote_node
                    .handle_chain_info_query(query)
                    .await?
                    .next_block_height
            }
        };
        let last = target_next_height
            .0
            .min(next_height.0.saturating_add(max_blocks));
        let heights = (next_height.0..last).map(BlockHeight).collect::<Vec<_>>();
        for chunk in heights.chunks(self.certificate_upload_batch_size as usize) {
            let certificates = self
                .storage
                .read_certificates_by_heights(chain_id, chunk)
                .await?;
            for certificate in certificates.into_iter().flatten() {
                // The validator's own responses move the cursor, so skip anything it has since
                // reported holding rather than re-sending it.
                if certificate.block().header.height < next_height {
                    continue;
                }
                let info = self.send_confirmed_certificate(&certificate, &[]).await?;
                next_height = info.next_block_height;
            }
        }
        Ok(next_height)
    }

    /// Sends one confirmed certificate, uploading blobs the validator reports missing.
    ///
    /// A missing *committee* (`EventsNotFound` for the epoch stream) is not recovered here: the
    /// admin chain is a chain like any other, so its own export brings the destination up to
    /// date, and this block succeeds on a later round. Replaying the admin chain from inside
    /// another chain's push is how one export round used to stall on an unbounded foreign
    /// history.
    async fn send_confirmed_certificate(
        &mut self,
        certificate: &CacheArc<ConfirmedBlockCertificate>,
        held: &[CacheArc<Blob>],
    ) -> Result<Box<crate::data_types::ChainInfo>, chain_client::Error> {
        let delivery = CrossChainMessageDelivery::NonBlocking;
        let mut result = self
            .remote_node
            .handle_optimized_confirmed_certificate(certificate, delivery)
            .await;
        // The same once-per-cause loop as the client's `RemoteNodeUpdater`: a second
        // `BlobsNotFound` naming new blobs is still recoverable, only repeating a cause is not.
        let mut sent_blobs = false;
        loop {
            match result {
                Err(NodeError::BlobsNotFound(blob_ids)) if !sent_blobs => {
                    self.remote_node
                        .check_blobs_not_found(certificate, &blob_ids)?;
                    let blobs = self.resolve_blobs(&blob_ids, held).await?;
                    self.remote_node
                        .node
                        .upload_blobs(blobs.into_iter().map(CacheArc::into_std).collect())
                        .await?;
                    sent_blobs = true;
                }
                result => return Ok(result?),
            }
            result = self
                .remote_node
                .handle_confirmed_certificate(certificate.clone(), delivery)
                .await;
        }
    }

    /// Collects the given blobs, taking each from `held` if present and the rest from storage.
    async fn resolve_blobs(
        &self,
        blob_ids: &[BlobId],
        held: &[CacheArc<Blob>],
    ) -> Result<Vec<CacheArc<Blob>>, chain_client::Error> {
        let mut blobs = Vec::with_capacity(blob_ids.len());
        let mut to_read = Vec::new();
        for blob_id in blob_ids {
            match held.iter().find(|blob| blob.id() == *blob_id) {
                Some(blob) => blobs.push(blob.clone()),
                None => to_read.push(*blob_id),
            }
        }
        if to_read.is_empty() {
            return Ok(blobs);
        }
        let read = self
            .storage
            .read_blobs(&to_read)
            .await?
            .into_iter()
            .collect::<Option<Vec<_>>>();
        blobs.extend(read.ok_or(NodeError::BlobsNotFound(to_read))?);
        Ok(blobs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every rejection in `check()` guards a distinct failure mode, so each invalid field must be
    /// caught on its own — including `max_retry_delay`, whose zero used to slip through and turn
    /// backoff into a busy retry.
    #[test]
    fn config_check_rejects_each_zero_knob() {
        assert!(BlockExportConfig::default().check().is_ok());
        let invalid = [
            BlockExportConfig {
                certificate_upload_batch_size: 0,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                queue_size: 0,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                queue_bytes: 0,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                max_in_flight_per_destination: 0,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                max_catch_up_blocks: 0,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                idle_catch_up_interval: Duration::ZERO,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                retry_delay: Duration::ZERO,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                max_retry_delay: Duration::ZERO,
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                retry_delay: Duration::from_secs(120),
                max_retry_delay: Duration::from_secs(60),
                ..BlockExportConfig::default()
            },
            BlockExportConfig {
                converged_chain_retention: Duration::ZERO,
                ..BlockExportConfig::default()
            },
        ];
        for config in invalid {
            assert!(config.check().is_err(), "accepted: {config:?}");
        }
    }

    /// A record starts from what the chain persisted, so a restart does not re-query every
    /// destination of every chain.
    ///
    /// This is the one place the persisted `exported_heights` enters the queue. It regressed
    /// twice — once when the seeding ran only at record creation and the tick could create
    /// records without it, once when adding the cursor-always-present invariant made the seeding
    /// a silent no-op — and neither showed up in any behavioural test, because a missing seed
    /// only costs a query.
    #[test]
    fn records_start_from_the_persisted_heights() {
        let validator = ValidatorPublicKey::test_key(1);
        let other = ValidatorPublicKey::test_key(2);
        let destinations: HashMap<ValidatorPublicKey, DestState<()>> =
            [(validator, test_dest_state()), (other, test_dest_state())]
                .into_iter()
                .collect();
        let exported = [(validator, BlockHeight(41))].into_iter().collect();

        let record = ChainRecord::new(Instant::now(), &destinations, &exported);

        assert_eq!(
            record.dests[&validator].next_height,
            Some(BlockHeight(42)),
            "a persisted height must seed the cursor for the block after it",
        );
        assert_eq!(
            record.dests[&other].next_height, None,
            "a destination with nothing persisted must be queried, not assumed",
        );
    }

    /// A cursor already present as `None` — the state a tick-created record starts in — is still
    /// seeded from the persisted heights.
    ///
    /// The seeding has regressed three times, each time because it was tied to record *creation*
    /// while a second path also creates records. This pins the property that actually matters:
    /// after the fill, a missing cursor is seeded no matter who made the record.
    #[test]
    fn a_cursor_left_unset_is_still_seeded() {
        let validator = ValidatorPublicKey::test_key(1);
        let destinations: HashMap<ValidatorPublicKey, DestState<()>> =
            [(validator, test_dest_state())].into_iter().collect();
        let exported: BTreeMap<_, _> = [(validator, BlockHeight(7))].into_iter().collect();

        // As the tick builds it: no heights to hand over, so the cursor starts unset.
        let mut record = ChainRecord::new(Instant::now(), &destinations, &BTreeMap::new());
        assert_eq!(record.dests[&validator].next_height, None);

        // The fill `on_block` performs once a block for that chain arrives.
        record.seed_missing_cursors(&destinations, &exported);

        assert_eq!(
            record.dests[&validator].next_height,
            Some(BlockHeight(8)),
            "a record the tick created must still pick up the persisted cursor",
        );
    }

    fn test_dest_state() -> DestState<()> {
        DestState {
            node: (),
            address: "grpc:localhost:1".to_string(),
            generation: 1,
            in_flight: 0,
            window: 1,
            retry_at: None,
            failures: 0,
            ready: VecDeque::new(),
        }
    }

    /// The backoff must escalate across consecutive failures — computing it from a reset counter
    /// is how it once retried a hopeless destination at the base delay forever.
    #[test]
    fn back_off_escalates_and_caps() {
        let config = BlockExportConfig {
            retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_millis(450),
            ..BlockExportConfig::default()
        };
        let mut failures = 0;
        let mut retry_at = None;
        let now = Instant::now();
        let mut delays = Vec::new();
        for _ in 0..4 {
            back_off(&mut failures, &mut retry_at, now, &config);
            delays.push(retry_at.expect("set by back_off") - now);
        }
        assert_eq!(
            delays,
            [
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(450),
            ],
        );
    }
}
