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
    collections::{BTreeMap, BTreeSet, HashMap},
    iter,
    sync::{Arc, Mutex},
};

use futures::{stream::FuturesUnordered, FutureExt as _, StreamExt as _};
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlockHeight, Epoch, TimeDelta, Timestamp},
    identifiers::{BlobId, ChainId, StreamId},
    time::{timer::timeout, Duration},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::{committee::Committee, system::EPOCH_STREAM_NAME};
use linera_storage::{Arc as CacheArc, Clock as _, Storage};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use crate::{
    client::chain_client,
    data_types::ChainInfoQuery,
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode, ValidatorNodeProvider},
    remote_node::RemoteNode,
};

#[cfg(with_metrics)]
pub(crate) mod metrics {
    use linera_base::prometheus_util::{
        exponential_bucket_interval, exponential_bucket_latencies, register_histogram,
        register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
        register_int_gauge_vec,
    };
    use prometheus::{Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec};

    linera_base::declare_metrics! {
        /// Sends refused for a reason about one chain rather than the destination — most often a
        /// committee the destination has not learned yet. Self-healing, so a rising rate is the
        /// signal, not the count: one that stays flat and non-zero means a destination is stuck on
        /// some chain and nothing is repairing it.
        pub static CHAIN_SCOPED_BACKOFFS: IntCounterVec =
            register_int_counter_vec(
                "block_export_chain_scoped_backoffs",
                "Sends deferred because a destination cannot accept a particular chain yet",
                &["validator"],
            );

        /// Chains the queue is tracking: those with a destination still behind, plus recently
        /// converged ones inside the retention window. A destination that is down holds every chain
        /// that produced a block during the outage here — that is the work-list its catch-up needs,
        /// and this is how an operator sees it growing.
        pub static TRACKED_CHAINS: IntGauge =
            register_int_gauge(
                "block_export_tracked_chains",
                "Chains the export queue is tracking for catch-up",
            );

        /// Blocks waiting in the export queue.
        pub static QUEUE_SIZE: IntGauge =
            register_int_gauge(
                "block_export_queue_size",
                "Blocks queued for export in this process",
            );

        /// Blob payload bytes held by queued blocks, since a block count alone hides the memory a
        /// backlog of large blobs pins.
        pub static QUEUE_BYTES: IntGauge =
            register_int_gauge(
                "block_export_queue_bytes",
                "Blob bytes held by blocks queued for export in this process",
            );

        /// Blocks dropped because the queue was full. Each is repaired by a later catch-up round, so
        /// this counting up means latency, not loss.
        pub static DROPPED_BLOCKS: IntCounter =
            register_int_counter(
                "block_export_dropped_blocks",
                "Blocks dropped from a full export queue, to be re-sent from storage",
            );

        /// Time from a block being queued to its sends being scheduled.
        pub static EXPORT_LATENCY: Histogram =
            register_histogram(
                "block_export_latency",
                "Time (ms) a block waits in the export queue before its sends are scheduled",
                exponential_bucket_latencies(60_000.0),
            );

        /// Time for one push to one destination, including any catch-up it triggered.
        pub static SEND_LATENCY: HistogramVec =
            register_histogram_vec(
                "block_export_send_latency",
                "Time (ms) to push one block to one destination validator",
                &["validator"],
                exponential_bucket_latencies(60_000.0),
            );

        /// How many concurrent sends each destination is currently allowed.
        pub static DESTINATION_WINDOW: IntGaugeVec =
            register_int_gauge_vec(
                "block_export_destination_window",
                "AIMD in-flight window per destination validator",
                &["validator"],
            );

        /// Destinations currently resolved. Zero with export enabled means the committee could not
        /// be loaded or no address resolved — on a dashboard that is otherwise indistinguishable
        /// from a healthy validator with nothing to send.
        pub static DESTINATIONS: IntGauge =
            register_int_gauge(
                "block_export_destinations",
                "Committee members this validator is currently exporting to",
            );

        /// Lagging (chain, destination) *pairs*, which is what the queue's memory tracks — a chain
        /// behind on ten destinations costs ten times one behind on one.
        pub static LAGGING_PAIRS: IntGauge =
            register_int_gauge(
                "block_export_lagging_pairs",
                "Chain-destination pairs currently behind, summed over destinations",
            );

        /// Blocks this validator still owes each destination, summed over every chain it is behind
        /// on. The aggregate backlog, which is what "is that validator caught up" actually asks.
        pub static BLOCKS_OWED: IntGaugeVec =
            register_int_gauge_vec(
                "block_export_blocks_owed",
                "Blocks still to send to a destination validator, summed over all chains",
                &["validator"],
            );

        /// The furthest behind any single chain is for a destination. A quantile over chains would
        /// need a per-pair observation, measured at 150 ms per sweep at a million pairs against
        /// 2 ms for this; and the maximum is the tail a quantile would hide anyway.
        pub static MAX_CHAIN_GAP: IntGaugeVec =
            register_int_gauge_vec(
                "block_export_max_chain_gap",
                "Blocks the furthest-behind chain owes a destination validator",
                &["validator"],
            );

        /// The queue-wide in-flight budget, halved whenever our own storage fails a read.
        pub static TOTAL_WINDOW: IntGauge =
            register_int_gauge(
                "block_export_total_window",
                "Concurrent sends allowed across all destinations (AIMD on local storage failures)",
            );

        /// Sends the destination actually answered, as opposed to attempts: `SEND_LATENCY` counts
        /// every completion, failures included, so success rate needs its own counter.
        pub static SENDS_SUCCEEDED: IntCounterVec =
            register_int_counter_vec(
                "block_export_sends_succeeded",
                "Export sends acknowledged by the destination validator",
                &["validator"],
            );

        /// How many blocks the destination was behind when we pushed to it: zero whenever the block
        /// was contiguous there, and the size of the gap we had to fill otherwise.
        pub static DESTINATION_LAG: HistogramVec =
            register_histogram_vec(
                "block_export_destination_lag",
                "Blocks a destination validator was missing when a block was pushed to it",
                &["validator"],
                exponential_bucket_interval(1.0, 10_000_000.0),
            );
    }
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
    /// The most concurrent sends across *all* destinations. Each one can be reading up to
    /// `max_catch_up_blocks` certificates, so without this the aggregate read concurrency is
    /// the per-destination window times the committee size, and nothing shrinks it when our own
    /// storage is the bottleneck.
    pub max_in_flight_total: usize,
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
        if self.max_in_flight_total == 0 {
            // The queue-wide budget would admit nothing, so no send would ever start.
            return Err("block export total in-flight budget must be greater than zero".into());
        }
        if self.max_in_flight_total < self.max_in_flight_per_destination {
            // One destination could never reach its own ceiling, and the AIMD window would
            // advertise a capacity the queue refuses to grant.
            return Err(
                "block export total in-flight budget must be at least the per-destination ceiling"
                    .into(),
            );
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
            max_in_flight_total: 64,
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
    /// Wall clock, not the storage clock: this only feeds the queue-latency histogram, and a
    /// simulated clock would report time that no operator waited.
    queued_at: linera_base::time::Instant,
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

/// A destination's dense index, assigned once per public key and never reused.
///
/// All per-chain state is keyed by this rather than by `ValidatorPublicKey`, which is 88 bytes
/// and whose `Ord` re-encodes the curve point on *every comparison* — measured at 36 ns, against
/// about 1 ns for an integer. Per-chain state is the one place that cost is multiplied by the
/// number of chains a down destination leaves behind.
type DestIndex = u32;

/// The highest height each destination has acknowledged, per chain, written by the queue task
/// and folded into each chain's `exported_heights` by its worker on save. Entries are removed
/// when a chain converges, so this holds lagging chains only.
#[derive(Default)]
struct ProgressMap {
    /// What the indices below refer to. Append-only: an index keeps its meaning for the life of
    /// the process, so a validator that leaves and rejoins cannot inherit another's cursors.
    validators: Vec<ValidatorPublicKey>,
    heights: HashMap<ChainId, Vec<(DestIndex, BlockHeight)>>,
}

impl ProgressMap {
    /// Drops the given chains, and returns a burst's peak-sized table for the caller to free
    /// *outside* the mutex.
    ///
    /// `remove` never shrinks, so a drained burst would otherwise pin its peak allocation
    /// forever. The obvious `shrink_to_fit` is not usable here: it frees the peak-sized bucket
    /// array in place, and this runs under the mutex every chain worker takes per executed block
    /// — whose hold `MAX_FORGET_PER_SWEEP` exists to bound. Rebuilding the few survivors into a
    /// fitted map costs within that budget; handing the old table back moves the
    /// peak-proportional free off the lock.
    fn forget_chains(
        &mut self,
        forgotten: &[ChainId],
    ) -> Option<HashMap<ChainId, Vec<(DestIndex, BlockHeight)>>> {
        for chain_id in forgotten {
            self.heights.remove(chain_id);
        }
        if self.heights.len() <= MAX_FORGET_PER_SWEEP
            && self.heights.capacity() > self.heights.len().saturating_mul(4)
        {
            let survivors = self.heights.drain().collect();
            return Some(std::mem::replace(&mut self.heights, survivors));
        }
        None
    }
}

type SharedProgress = Arc<Mutex<ProgressMap>>;

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
            queued_at: linera_base::time::Instant::now(),
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
        let Some(chain_progress) = progress.heights.get(&chain_id) else {
            return BTreeMap::new();
        };
        chain_progress
            .iter()
            .filter_map(|(index, height)| {
                let validator = progress.validators.get(*index as usize)?;
                committee
                    .validators()
                    .contains_key(validator)
                    .then_some((*validator, *height))
            })
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
    let max_in_flight_total = config.max_in_flight_total;

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
        ticks_until_sweep: TICKS_PER_CONVERGENCE_SWEEP,
        #[cfg(with_metrics)]
        ticks_until_census: 0,
        drain_cursor: None,
        chains: HashMap::new(),
        destinations: BTreeMap::new(),
        dest_indices: BTreeMap::new(),
        next_generation: 0,
        total_window: max_in_flight_total,
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
    last_activity: Timestamp,
    /// Sorted by index, so lookups binary-search integers. A flat vector rather than a map
    /// because this is allocated per tracked chain: a `BTreeMap` pays for an eleven-slot leaf
    /// whatever the committee size.
    dests: Vec<(DestIndex, ChainDest)>,
}

impl ChainRecord {
    /// Starts a record with a cursor for every current destination.
    ///
    /// Seeding here rather than at the call sites is what makes the empty-cursor state
    /// unrepresentable: a record with no cursors is invisible to the requeue loop and trivially
    /// "converged" to the sweep, so such a chain would be abandoned in silence. One creation
    /// path used to miss the seeding, and nothing in the suite could see it.
    fn new<N>(
        now: Timestamp,
        destinations: &BTreeMap<DestIndex, DestState<N>>,
        exported_heights: &BTreeMap<ValidatorPublicKey, BlockHeight>,
    ) -> Self {
        ChainRecord {
            tip: BlockHeight::ZERO,
            last_activity: now,
            // Already in index order, which is the order `dests` must keep.
            dests: destinations
                .iter()
                .map(|(index, dest)| {
                    // Seeded here rather than by a later fill: a cursor pre-populated as `None`
                    // and then only `or_insert`-ed would silently swallow the persisted height,
                    // leaving `exported_heights` write-only and costing a query per destination
                    // on every restart.
                    let next_height = exported_heights
                        .get(&dest.validator)
                        .and_then(|height| height.try_add_one().ok());
                    (
                        *index,
                        ChainDest {
                            next_height,
                            ..ChainDest::default()
                        },
                    )
                })
                .collect(),
        }
    }

    fn dest(&self, index: DestIndex) -> Option<&ChainDest> {
        let at = self
            .dests
            .binary_search_by_key(&index, |(at, _)| *at)
            .ok()?;
        Some(&self.dests[at].1)
    }

    fn dest_mut(&mut self, index: DestIndex) -> Option<&mut ChainDest> {
        let at = self
            .dests
            .binary_search_by_key(&index, |(at, _)| *at)
            .ok()?;
        Some(&mut self.dests[at].1)
    }

    /// The cursor for `index`, created empty if this record does not have one yet.
    fn dest_entry(&mut self, index: DestIndex) -> &mut ChainDest {
        let at = match self.dests.binary_search_by_key(&index, |(at, _)| *at) {
            Ok(at) => at,
            Err(at) => {
                self.dests.insert(at, (index, ChainDest::default()));
                at
            }
        };
        &mut self.dests[at].1
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
        destinations: &BTreeMap<DestIndex, DestState<N>>,
        exported_heights: &BTreeMap<ValidatorPublicKey, BlockHeight>,
    ) {
        for (index, dest) in destinations {
            let chain_dest = self.dest_entry(*index);
            // Guard first: this runs per block, and in the steady state every cursor is set, so
            // the pubkey lookup would be pure waste.
            if chain_dest.next_height.is_none() && chain_dest.in_flight.is_none() {
                chain_dest.next_height = exported_heights
                    .get(&dest.validator)
                    .and_then(|height| height.try_add_one().ok());
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
    /// Backoff for *chain-scoped* failures — the destination is healthy but cannot accept this
    /// chain yet, e.g. it lacks the committee and the admin chain's export has not reached it.
    retry_at: Option<Timestamp>,
    failures: u32,
    /// How many times this destination has reported a *lower* height than it had. Counted apart
    /// from `failures` because an advance clears those, and a peer alternating advance with
    /// regression would otherwise reset its own penalty on every second answer and never
    /// escalate. Fits in `ChainDest`'s existing padding.
    regressions: u32,
}

impl ChainDest {
    /// Folds in a height the destination reported, returning the height to record as
    /// acknowledged, if any.
    ///
    /// The validator is authoritative about its own height in *both* directions: only one send
    /// per pair is ever in flight and stale generations are filtered out, so a lower report is
    /// not a race. One restored from a backup reports lower, and refusing to believe it would
    /// leave that gap unrepaired for good — we would think it caught up and never re-send what
    /// it lost. Believing it is therefore required; paying for it is what stops a peer from
    /// lying its way into an unthrottled re-send loop.
    fn record_reached(
        &mut self,
        reported: BlockHeight,
        tip: BlockHeight,
        now: Timestamp,
        config: &BlockExportConfig,
    ) -> Option<BlockHeight> {
        // Clamped once, here, so the cursor, the counters and the acknowledgement all read the
        // same height. A destination legitimately runs ahead of us — the client broadcasts to
        // everyone — but the value is *its* claim, and storing a claim above our tip satisfies
        // none of the "behind" predicates that schedule work, while satisfying every
        // "converged" one. Since only a completed send can rewrite the cursor, and no send is
        // ever scheduled for a pair that looks converged, an over-report would strand that pair
        // for the life of the process. Clamped, it is self-correcting: the pair re-enters the
        // work set as soon as our own tip passes the clamp.
        let reported = reported.min(tip);
        let previous = self.next_height;
        let advanced = previous.is_none_or(|height| reported > height);
        let regressed = previous.is_some_and(|height| reported < height);
        self.next_height = Some(reported);
        if advanced {
            self.failures = 0;
            self.retry_at = None;
        } else if regressed {
            // Escalates on the regression count, which an advance does not clear: a genuine
            // restore regresses once and pays one delay, an oscillating peer pays double each
            // time it lies.
            let attempt = self.failures.max(self.regressions);
            self.retry_at = Some(now.saturating_add(backoff_delay(attempt, config)));
            self.regressions = self.regressions.saturating_add(1);
        } else if reported < tip {
            // Answered but moved nothing — a gap our storage cannot fill — so back this pair
            // off rather than spinning on it.
            back_off(&mut self.failures, &mut self.retry_at, now, config);
            return None;
        } else {
            return None;
        }
        reported.try_sub_one().ok()
    }
}

/// One destination validator: its connection and the health state every chain shares.
struct DestState<N> {
    node: N,
    /// Kept here because per-chain state refers to this destination by index, not by key.
    validator: ValidatorPublicKey,
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
    retry_at: Option<Timestamp>,
    failures: u32,
    /// Chains this destination is behind on, maintained as pairs fall behind and converge
    /// rather than rediscovered by scanning every chain each tick — that scan was O(tracked
    /// chains x destinations) at 5 Hz, which at a million lagging chains took longer than the
    /// tick interval itself and starved the queue of everything else.
    ///
    /// Membership *is* the "needs work" flag, so there is no separate `queued` bit to fall out
    /// of step with it.
    lagging: BTreeSet<ChainId>,
    /// Where the next drain resumes in `lagging`. Without it the set's ordering hands the window
    /// to the same lowest chain ids every time and starves the rest of the backlog — the
    /// fairness the previous FIFO had for free.
    lagging_cursor: Option<ChainId>,
}

impl<N> DestState<N> {
    /// The chains to consider this round, resuming where the last one stopped and wrapping around.
    ///
    /// The rotation is the point: `lagging` is ordered, so walking it from the start every tick
    /// would hand the window to the same lowest chain ids forever and starve the rest.
    fn drain_candidates(&self, budget: usize) -> Vec<ChainId> {
        match self.lagging_cursor {
            Some(cursor) => self
                .lagging
                .range(cursor..)
                .chain(self.lagging.iter().take_while(|id| **id < cursor))
                .copied()
                .take(budget)
                .collect(),
            None => self.lagging.iter().copied().take(budget).collect(),
        }
    }

    /// Moves the cursor past the last chain considered, so the next round advances.
    ///
    /// A round that considered nothing — a saturated destination breaks before looking at its
    /// first candidate — leaves the cursor alone. Clearing it there restarts the next drain at
    /// the lowest chain id, which is the starvation the cursor exists to prevent, and a busy
    /// destination is saturated on almost every tick.
    fn advance_cursor(&mut self, last_considered: Option<ChainId>) {
        let Some(last) = last_considered else {
            return;
        };
        self.lagging_cursor = self.lagging.range(last..).nth(1).copied();
    }
}

/// How one send ended, scoped to what the error tells us about.
enum SendOutcome {
    /// The validator answered; its reported next height.
    Reached(BlockHeight),
    /// The failure is about this chain on this destination, not about the destination.
    ChainScoped(Box<chain_client::Error>),
    /// The failure is about the destination itself.
    DestinationScoped(Box<chain_client::Error>),
    /// Our own storage failed. Nobody's health signal but ours.
    LocalScoped(Box<chain_client::Error>),
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
    /// Ticks until the next sweep of converged chains.
    ticks_until_sweep: u32,
    /// Ticks until the next backlog census.
    #[cfg(with_metrics)]
    ticks_until_census: u32,
    /// Which destination the queue-wide budget is offered to first, rotated every tick.
    drain_cursor: Option<DestIndex>,
    chains: HashMap<ChainId, ChainRecord>,
    destinations: BTreeMap<DestIndex, DestState<P::Node>>,
    /// Every validator ever registered as a destination, and the index its per-chain state uses.
    /// Never pruned, so a validator that rejoins reuses its index rather than taking a departed
    /// one's.
    dest_indices: BTreeMap<ValidatorPublicKey, DestIndex>,
    /// The last destination generation handed out; never reused within this queue's lifetime.
    next_generation: u64,
    /// Concurrent sends allowed across all destinations right now: halved when our own storage
    /// fails a read, restored one slot per success up to `max_in_flight_total`. A destination's
    /// own window bounds what one peer can consume; this bounds what the queue as a whole asks
    /// of storage.
    total_window: usize,
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

/// How many times the window a single drain may look past before giving up for this tick, so a
/// backlog of ineligible entries cannot turn the drain back into a full scan.
const LAGGING_SCAN_FACTOR: usize = 4;

/// How many ticks apart converged chains are swept. The window they are held for is minutes, so
/// this only decides how promptly the memory comes back.
const TICKS_PER_CONVERGENCE_SWEEP: u32 = 25;

/// How many ticks apart the backlog census runs. Slower than the sweep because it costs the size
/// of the real backlog and answers a dashboard question, not a scheduling one.
#[cfg(with_metrics)]
const TICKS_PER_BACKLOG_CENSUS: u32 = 300;

/// Destinations in the order the queue-wide budget is offered to them, resuming past `cursor`
/// and wrapping. Rotating matters because the budget is shared: served in index order every
/// time, the first `max_in_flight_total / max_in_flight_per_destination` destinations absorb all
/// of it and the rest never get a catch-up slot.
fn rotated_order(indices: &[DestIndex], cursor: Option<DestIndex>) -> Vec<DestIndex> {
    match cursor {
        Some(at) => indices
            .iter()
            .copied()
            .skip_while(|index| *index < at)
            .chain(indices.iter().copied().take_while(|index| *index < at))
            .collect(),
        None => indices.to_vec(),
    }
}

/// Where the next round starts: past the destination this one served first.
fn next_drain_cursor(indices: &[DestIndex], served_first: Option<DestIndex>) -> Option<DestIndex> {
    let first = served_first?;
    indices.iter().copied().find(|index| *index > first)
}

/// The most (chain, destination) pairs one census walks. Each is a random lookup into the chain
/// map plus a search of its cursors — measured at roughly half a microsecond per pair, so this
/// caps the stall at about 25 ms however deep the backlog is. Past the cap the gauges are a
/// floor rather than a total, which is the right trade for a number read off a dashboard:
/// `block_export_lagging_pairs` is exact and free, and says how far past the cap we are.
#[cfg(with_metrics)]
const MAX_CENSUS_PAIRS: usize = 50_000;

/// How many chains one sweep may forget, bounding how long it holds the progress mutex that every
/// chain worker takes on every block.
const MAX_FORGET_PER_SWEEP: usize = 4096;

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
        let tick_delta = TimeDelta::from_micros(interval.as_micros() as u64);
        let mut next_tick = self
            .storage
            .clock()
            .current_time()
            .saturating_add(tick_delta);
        loop {
            let now = self.storage.clock().current_time();
            // The storage clock is the wall clock in production, so it can step backwards (NTP,
            // a restored snapshot). The deadline is a timestamp but the wait is real time, so a
            // backward step of N seconds would otherwise suspend every tick-only duty — backoff
            // expiry, drop repair, the committee scan — for N seconds. Never wait longer than
            // one interval.
            if next_tick.duration_since(now) > interval {
                next_tick = now.saturating_add(tick_delta);
            }
            if now >= next_tick {
                self.tick(&mut jobs).await;
                next_tick = self
                    .storage
                    .clock()
                    .current_time()
                    .saturating_add(tick_delta);
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
                    _ = self.storage.clock().sleep_for(until_tick).fuse() => Wake::Tick,
                }
            };
            match wake {
                Wake::Done(done) => self.on_done(done, &mut jobs),
                Wake::Block(Some(block)) => self.on_block(block, &mut jobs),
                Wake::Block(None) => break,
                Wake::Tick => {
                    self.tick(&mut jobs).await;
                    next_tick = self
                        .storage
                        .clock()
                        .current_time()
                        .saturating_add(tick_delta);
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

        let now = self.storage.clock().current_time();
        let tip = height.try_add_one().unwrap_or(BlockHeight::MAX);
        let record = self
            .chains
            .entry(chain_id)
            .or_insert_with(|| ChainRecord::new(now, &self.destinations, &block.exported_heights));
        record.tip = record.tip.max(tip);
        record.last_activity = now;
        record.seed_missing_cursors(&self.destinations, &block.exported_heights);
        let indices = self.destinations.keys().copied().collect::<Vec<_>>();
        for index in indices {
            let budget = self.budget_remaining();
            let record = self.chains.get_mut(&chain_id).expect("inserted above");
            let record_tip = record.tip;
            let chain_dest = record.dest_entry(index);
            let dest = self
                .destinations
                .get_mut(&index)
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
                    index,
                    dest,
                    chain_dest,
                    record_tip,
                    Some((block.certificate.clone(), block.blobs.clone())),
                );
            } else if chain_dest.next_height.is_none_or(|next| next < record_tip) {
                dest.lagging.insert(chain_id);
                if can_send_now {
                    Self::drain_ready(
                        &mut self.chains,
                        &self.storage,
                        &self.config,
                        index,
                        dest,
                        jobs,
                        now,
                        budget,
                    );
                }
            }
        }
    }

    /// Folds one finished send back into the destination's and the chain's state.
    fn on_done(
        &mut self,
        (chain_id, index, generation, outcome): JobDone,
        jobs: &mut FuturesUnordered<JobFuture>,
    ) {
        let now = self.storage.clock().current_time();
        let Some(dest) = self.destinations.get_mut(&index) else {
            return; // The validator left the committee while its send was in flight.
        };
        let validator = dest.validator;
        if dest.generation != generation {
            // The send ran against a previous incarnation of this destination; its slot was
            // never counted here and its result must not touch the fresh state.
            if let Some(chain_dest) = self
                .chains
                .get_mut(&chain_id)
                .and_then(|record| record.dest_mut(index))
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
                self.total_window = (self.total_window + 1).min(self.config.max_in_flight_total);
                #[cfg(with_metrics)]
                metrics::SENDS_SUCCEEDED
                    .with_label_values(&[&dest.address])
                    .inc();
            }
            SendOutcome::ChainScoped(_) => {}
            SendOutcome::LocalScoped(error) => {
                // Our storage, not the peer: leave the destination's window alone and halve the
                // queue's own budget, so the pressure is relieved across every destination
                // rather than one pair at a time while the rest keep reading.
                warn!(
                    %chain_id, %error,
                    "Export could not read from local storage; halving the queue's total \
                     in-flight budget",
                );
                self.total_window = (self.total_window / 2).max(1);
                #[cfg(with_metrics)]
                metrics::TOTAL_WINDOW.set(self.total_window as i64);
            }
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
            let record_tip = record.tip;
            if let Some(chain_dest) = record.dest_mut(index) {
                if chain_dest.in_flight == Some(generation) {
                    chain_dest.in_flight = None;
                }
                match &outcome {
                    SendOutcome::Reached(next_height) => {
                        if let Some(acked) =
                            chain_dest.record_reached(*next_height, record_tip, now, &self.config)
                        {
                            let mut progress = self
                                .progress
                                .lock()
                                .expect("progress mutex is never poisoned");
                            let heights = progress.heights.entry(chain_id).or_default();
                            match heights.binary_search_by_key(&index, |(at, _)| *at) {
                                Ok(at) => heights[at].1 = acked,
                                Err(at) => heights.insert(at, (index, acked)),
                            }
                        }
                    }
                    SendOutcome::LocalScoped(_) => {
                        // The global budget already shrank; back the pair off too so the same
                        // unreadable range is not retried immediately.
                        back_off(
                            &mut chain_dest.failures,
                            &mut chain_dest.retry_at,
                            now,
                            &self.config,
                        );
                    }
                    SendOutcome::ChainScoped(error) => {
                        debug!(
                            %chain_id, %validator, %error,
                            "Destination cannot accept this chain yet; backing the pair off",
                        );
                        #[cfg(with_metrics)]
                        metrics::CHAIN_SCOPED_BACKOFFS
                            .with_label_values(&[&dest.address])
                            .inc();
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
        let budget = self.budget_remaining();
        let dest = self.destinations.get_mut(&index).expect("checked above");
        if let Some(record) = self.chains.get_mut(&chain_id) {
            let tip = record.tip;
            if let Some(chain_dest) = record.dest_mut(index) {
                // Membership tracks "behind", so convergence removes it and nothing else has
                // to remember to.
                if chain_dest.next_height.is_none_or(|next| next < tip) {
                    dest.lagging.insert(chain_id);
                } else {
                    dest.lagging.remove(&chain_id);
                }
            }
        }
        Self::drain_ready(
            &mut self.chains,
            &self.storage,
            &self.config,
            index,
            dest,
            jobs,
            now,
            budget,
        );
    }

    /// An idle moment: pick up committee changes from storage and requeue expired backoffs.
    async fn tick(&mut self, jobs: &mut FuturesUnordered<JobFuture>) {
        let now = self.storage.clock().current_time();
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
            let advanced = record.tip < tip;
            record.tip = record.tip.max(tip);
            if advanced {
                // The scan that used to notice this is gone, so a tip moving forward records
                // the chains it just put behind, here and now.
                for (index, dest) in &mut self.destinations {
                    let chain_dest = record.dest_entry(*index);
                    if chain_dest.next_height.is_none_or(|next| next < record.tip) {
                        dest.lagging.insert(chain_id);
                    }
                }
            }
        }
        // When the destination set changed, every destination gets a cursor on every tracked
        // chain, so a validator that joined after a chain's last block is still caught up on it.
        // Gated on the change: this walks every record.
        if self.destinations_changed {
            self.destinations_changed = false;
            // The one place a full pass is unavoidable: a destination that just joined has no
            // idea which chains it is behind on. It runs per committee change, not per tick.
            for (chain_id, record) in &mut self.chains {
                for (index, dest) in &mut self.destinations {
                    let chain_dest = record.dest_entry(*index);
                    if chain_dest.next_height.is_none_or(|next| next < record.tip) {
                        dest.lagging.insert(*chain_id);
                    }
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
        // On its own cadence: this walks every tracked chain, while the retention window it
        // enforces is measured in minutes. Running it per tick spent a quarter of a core at
        // 100k chains to reclaim memory a few seconds sooner.
        if self.ticks_until_sweep == 0 {
            self.ticks_until_sweep = TICKS_PER_CONVERGENCE_SWEEP;
            self.chains.retain(|chain_id, record| {
                // Bounded per sweep: the removals below are what the chain workers block on, so
                // the mutex hold has to be a constant, not a function of how much converged at
                // once. The remainder goes on the next sweep.
                if forgotten.len() >= MAX_FORGET_PER_SWEEP {
                    return true;
                }
                let converged = destinations.keys().all(|index| {
                    record.dest(*index).is_some_and(|chain_dest| {
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
            // `retain` never shrinks the table, so without this a one-off burst of chains would
            // hold its peak allocation for the life of the process.
            if self.chains.capacity() > self.chains.len().saturating_mul(4) {
                self.chains.shrink_to_fit();
            }
        } else {
            self.ticks_until_sweep -= 1;
        }
        if !forgotten.is_empty() {
            let peak_table = self
                .progress
                .lock()
                .expect("progress mutex is never poisoned")
                .forget_chains(&forgotten);
            // The free of a burst's peak-sized table happens here, after the guard above is
            // gone — measured at milliseconds per gigabyte-scale table, which is fine for the
            // queue task and was not fine under the mutex.
            drop(peak_table);
        }

        #[cfg(with_metrics)]
        {
            metrics::TRACKED_CHAINS.set(self.chains.len() as i64);
            metrics::DESTINATIONS.set(self.destinations.len() as i64);
            let lagging_pairs = self
                .destinations
                .values()
                .map(|dest| dest.lagging.len())
                .sum::<usize>();
            metrics::LAGGING_PAIRS.set(lagging_pairs as i64);
            metrics::TOTAL_WINDOW.set(self.total_window as i64);
            if self.ticks_until_census == 0 {
                self.ticks_until_census = TICKS_PER_BACKLOG_CENSUS;
                self.publish_backlog();
            } else {
                self.ticks_until_census -= 1;
            }
        }

        // No requeue scan: each destination's `lagging` set already *is* the list of chains it
        // owes work on, kept current as pairs fall behind and converge. Draining it is
        // proportional to the work available, not to how much state the process is holding.
        let mut budget = self.total_window.saturating_sub(
            self.destinations
                .values()
                .map(|dest| dest.in_flight)
                .sum::<usize>(),
        );
        // Resume where the last round stopped. The budget is queue-wide, so serving destinations
        // in index order every time lets the first `total_window / window` of them absorb all of
        // it — at a committee larger than that ratio (8 with the defaults) the rest would get no
        // catch-up at all. Same starvation the per-chain cursor exists to prevent, one level up.
        let indices = self.destinations.keys().copied().collect::<Vec<_>>();
        let order = rotated_order(&indices, self.drain_cursor);
        self.drain_cursor = next_drain_cursor(&indices, order.first().copied());
        for index in order {
            let Some(dest) = self.destinations.get_mut(&index) else {
                continue;
            };
            budget -= Self::drain_ready(
                &mut self.chains,
                &self.storage,
                &self.config,
                index,
                dest,
                jobs,
                now,
                budget,
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
        // Cloned so the index registry below can be updated while the committee is read.
        let Some(committee) = self.committee.clone() else {
            return;
        };
        self.committee_dirty = false;
        // A destination whose address merely changed keeps the backlog it had accumulated: it is
        // the same peer, and rediscovering that list costs a pass over every tracked chain.
        let mut carried = BTreeMap::new();
        let mut rebuilt_any = false;
        #[cfg(with_metrics)]
        let mut rebuilt_addresses = Vec::new();
        self.destinations.retain(|index, dest| {
            let keep = committee
                .validators()
                .get(&dest.validator)
                .is_some_and(|state| state.network_address == dest.address);
            if !keep {
                rebuilt_any = true;
                if committee.validators().contains_key(&dest.validator) {
                    carried.insert(
                        *index,
                        (
                            std::mem::take(&mut dest.lagging),
                            dest.lagging_cursor.take(),
                        ),
                    );
                }
                #[cfg(with_metrics)]
                rebuilt_addresses.push(dest.address.clone());
            }
            keep
        });
        if rebuilt_any {
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
            metrics::SENDS_SUCCEEDED
                .remove_label_values(&[address])
                .ok();
            metrics::DESTINATION_LAG
                .remove_label_values(&[address])
                .ok();
            metrics::CHAIN_SCOPED_BACKOFFS
                .remove_label_values(&[address])
                .ok();
            metrics::BLOCKS_OWED.remove_label_values(&[address]).ok();
            metrics::MAX_CHAIN_GAP.remove_label_values(&[address]).ok();
        }
        for (validator, address) in committee.validator_addresses() {
            if Some(validator) == self.own_public_key {
                continue;
            }
            let index = self.dest_index(validator);
            if self.destinations.contains_key(&index) {
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
                        let (lagging, lagging_cursor) = carried.remove(&index).unwrap_or_default();
                        self.destinations.insert(
                            index,
                            DestState {
                                node,
                                validator,
                                address: address.to_owned(),
                                generation: self.next_generation,
                                in_flight: 0,
                                window: self.config.max_in_flight_per_destination,
                                retry_at: None,
                                failures: 0,
                                lagging,
                                lagging_cursor,
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
        let destinations = &self.destinations;
        for record in self.chains.values_mut() {
            record
                .dests
                .retain(|(index, _)| destinations.contains_key(index));
        }
    }

    /// Publishes how far behind each destination is, walking the `lagging` sets rather than
    /// every tracked chain.
    ///
    /// Those sets already name exactly the chains a destination owes blocks on, so this costs
    /// the size of the real backlog: nothing when everyone is caught up, and proportional to the
    /// outage when they are not. Folding it into the convergence sweep instead would have cost
    /// that sweep its short-circuit — measured at +142 ms per sweep at 100k chains and a
    /// 20-member committee, against 9 ms for this.
    ///
    /// The maximum rather than a quantile: a per-pair histogram observation measured 150 ms per
    /// sweep at a million pairs, and the maximum is the tail a quantile would hide anyway.
    #[cfg(with_metrics)]
    fn publish_backlog(&self) {
        // Shared across destinations so one enormous backlog cannot spend the whole budget and
        // leave every later destination reporting zero.
        let mut remaining = MAX_CENSUS_PAIRS;
        let mut unvisited = self.destinations.len();
        for (index, dest) in &self.destinations {
            let mut owed = 0u64;
            let mut worst = 0u64;
            // Divided by the destinations still to come, not by all of them: dividing by the
            // total while `remaining` shrinks gives each successive destination a smaller share
            // than the last, so the ones late in the index order would under-report their
            // backlog. This way a destination that needs less than its share leaves the surplus
            // to the rest, and the last one may use whatever is left.
            let per_destination = remaining / unvisited.max(1);
            unvisited = unvisited.saturating_sub(1);
            let mut examined = 0usize;
            for chain_id in &dest.lagging {
                if examined >= per_destination {
                    break;
                }
                examined += 1;
                let Some(record) = self.chains.get(chain_id) else {
                    continue;
                };
                let Some(chain_dest) = record.dest(*index) else {
                    continue;
                };
                let gap = chain_dest
                    .next_height
                    .map_or(record.tip.0, |next| record.tip.0.saturating_sub(next.0));
                owed = owed.saturating_add(gap);
                worst = worst.max(gap);
            }
            // Published for every destination, not just the ones behind: a gauge left at its last
            // value would read as a permanent debt after the peer caught up.
            metrics::BLOCKS_OWED
                .with_label_values(&[&dest.address])
                .set(owed as i64);
            metrics::MAX_CHAIN_GAP
                .with_label_values(&[&dest.address])
                .set(worst as i64);
            remaining = remaining.saturating_sub(examined);
        }
    }

    /// Sends the queue-wide budget still allows, summed from the destinations rather than
    /// tracked in a counter that could drift out of step with them.
    fn budget_remaining(&self) -> usize {
        let in_flight = self
            .destinations
            .values()
            .map(|dest| dest.in_flight)
            .sum::<usize>();
        self.total_window.saturating_sub(in_flight)
    }

    /// The index `validator`'s per-chain state is keyed by, registering it on first sight.
    ///
    /// Published to the handles under the progress mutex, because the chain workers read that
    /// map back and only the registry can name the validators in it.
    fn dest_index(&mut self, validator: ValidatorPublicKey) -> DestIndex {
        if let Some(index) = self.dest_indices.get(&validator) {
            return *index;
        }
        let mut progress = self
            .progress
            .lock()
            .expect("progress mutex is never poisoned");
        let index = progress.validators.len() as DestIndex;
        progress.validators.push(validator);
        self.dest_indices.insert(validator, index);
        index
    }
}

/// The result of one send job: which pair it was for, under which destination generation, and
/// how it went.
type JobDone = (ChainId, DestIndex, u64, SendOutcome);

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
        index: DestIndex,
        dest: &mut DestState<P::Node>,
        chain_dest: &mut ChainDest,
        target: BlockHeight,
        live: Option<(CacheArc<ConfirmedBlockCertificate>, Vec<CacheArc<Blob>>)>,
    ) {
        let generation = dest.generation;
        chain_dest.in_flight = Some(generation);
        dest.in_flight += 1;
        let mut sender = BlockSender {
            remote_node: RemoteNode {
                public_key: dest.validator,
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
                Err(error) if is_local_scoped(&error) => SendOutcome::LocalScoped(Box::new(error)),
                Err(error) if is_chain_scoped(&error) => SendOutcome::ChainScoped(Box::new(error)),
                Err(error) => SendOutcome::DestinationScoped(Box::new(error)),
            };
            (chain_id, index, generation, outcome)
        };
        #[cfg(not(web))]
        jobs.push(job.boxed());
        #[cfg(web)]
        jobs.push(job.boxed_local());
    }

    /// Starts catch-up sends from this destination's ready list until its window is full.
    #[expect(clippy::too_many_arguments)]
    fn drain_ready(
        chains: &mut HashMap<ChainId, ChainRecord>,
        storage: &S,
        config: &BlockExportConfig,
        index: DestIndex,
        dest: &mut DestState<P::Node>,
        jobs: &mut FuturesUnordered<JobFuture>,
        now: Timestamp,
        budget: usize,
    ) -> usize {
        if dest.retry_at.is_some_and(|at| at > now) || dest.in_flight >= dest.window || budget == 0
        {
            return 0;
        }
        // Only as far as the window allows, so the cost is the sends we are about to make and
        // not the size of the backlog. Entries stay in `lagging` until they converge: one that
        // is mid-send or serving its own backoff is skipped here and picked up by a later tick,
        // with nothing to remember to re-add it.
        // From the cursor onwards, then wrapping to the start: every chain gets its turn even
        // when the backlog is far larger than the window. Bounded by a multiple of the window so
        // a backlog of ineligible entries cannot turn this back into a full scan.
        let ordered = dest.drain_candidates(dest.window.saturating_mul(LAGGING_SCAN_FACTOR));
        let mut spawn = Vec::new();
        let mut stale = Vec::new();
        let mut last_visited = None;
        for chain_id in ordered {
            if dest.in_flight + spawn.len() >= dest.window || spawn.len() >= budget {
                break;
            }
            last_visited = Some(chain_id);
            let Some(record) = chains.get(&chain_id) else {
                // The chain was forgotten under us; drop the entry rather than walk past it on
                // every drain from here on.
                stale.push(chain_id);
                continue;
            };
            let Some(chain_dest) = record.dest(index) else {
                continue;
            };
            let behind = chain_dest.next_height.is_none_or(|next| next < record.tip);
            if behind
                && chain_dest.in_flight.is_none()
                && chain_dest.retry_at.is_none_or(|at| at <= now)
            {
                spawn.push(chain_id);
            }
        }
        dest.advance_cursor(last_visited);
        for chain_id in stale {
            dest.lagging.remove(&chain_id);
        }
        let spawned = spawn.len();
        for chain_id in spawn {
            let Some(record) = chains.get_mut(&chain_id) else {
                continue;
            };
            let tip = record.tip;
            let Some(chain_dest) = record.dest_mut(index) else {
                continue;
            };
            Self::spawn_job(
                jobs, storage, config, chain_id, index, dest, chain_dest, tip, None,
            );
        }
        spawned
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
        )
    )
}

/// Whether this failure is our own storage rather than anything about the destination.
///
/// Halving the destination's window for it would punish the wrong side, but backing off only the
/// one pair leaves every other pair reading at full rate — so the control loop cannot see the
/// bottleneck it is creating. These shrink the queue's *global* budget instead.
fn is_local_scoped(error: &chain_client::Error) -> bool {
    matches!(
        error,
        chain_client::Error::ReadCertificatesError(_) | chain_client::Error::ViewError(_)
    )
}

/// Escalating backoff shared by both scopes: doubles from `retry_delay` per consecutive failure,
/// capped at `max_retry_delay`.
fn back_off(
    failures: &mut u32,
    retry_at: &mut Option<Timestamp>,
    now: Timestamp,
    config: &BlockExportConfig,
) {
    *retry_at = Some(now.saturating_add(backoff_delay(*failures, config)));
    *failures = failures.saturating_add(1);
}

/// The delay after `attempt` consecutive failures: doubles per attempt, capped.
fn backoff_delay(attempt: u32, config: &BlockExportConfig) -> TimeDelta {
    let delay = config
        .retry_delay
        .saturating_mul(1u32.checked_shl(attempt).unwrap_or(u32::MAX))
        .min(config.max_retry_delay);
    TimeDelta::from_micros(delay.as_micros() as u64)
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
    ///
    /// The pair retries indefinitely — the backoff caps at `max_retry_delay` — so it recovers
    /// whenever the destination learns the epoch, from the admin chain's own export or from the
    /// client, which pushes it on this same error. After a restart the admin chain re-enters
    /// this queue's work-list only once it produces a block, so a quiet admin chain can leave a
    /// pair deferred for a while; `CHAIN_SCOPED_BACKOFFS` is what makes that visible.
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
    use linera_base::crypto::CryptoHash;

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
        let destinations = test_destinations([validator, other]);
        let exported = [(validator, BlockHeight(41))].into_iter().collect();

        let record = ChainRecord::new(Timestamp::now(), &destinations, &exported);

        assert_eq!(
            record.dest(0).unwrap().next_height,
            Some(BlockHeight(42)),
            "a persisted height must seed the cursor for the block after it",
        );
        assert_eq!(
            record.dest(1).unwrap().next_height,
            None,
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
        let destinations = test_destinations([validator]);
        let exported: BTreeMap<_, _> = [(validator, BlockHeight(7))].into_iter().collect();

        // As the tick builds it: no heights to hand over, so the cursor starts unset.
        let mut record = ChainRecord::new(Timestamp::now(), &destinations, &BTreeMap::new());
        assert_eq!(record.dest(0).unwrap().next_height, None);

        // The fill `on_block` performs once a block for that chain arrives.
        record.seed_missing_cursors(&destinations, &exported);

        assert_eq!(
            record.dest(0).unwrap().next_height,
            Some(BlockHeight(8)),
            "a record the tick created must still pick up the persisted cursor",
        );
    }

    /// The drain rotates through the backlog instead of always serving its lowest chain ids.
    ///
    /// The maintained set replaced a FIFO, and a set is *ordered* — walking it from the start
    /// every tick hands the whole window to the same few chains and starves everything after
    /// them, which at a large backlog means those chains are never exported at all.
    #[test]
    fn draining_rotates_through_the_backlog() {
        let mut dest = test_dest_state();
        dest.lagging = test_chain_ids(6).into_iter().collect();

        // Two rounds of a two-chain window; the second must move past the first.
        let first = dest.drain_candidates(2);
        dest.advance_cursor(first.last().copied());
        let second = dest.drain_candidates(2);

        assert_eq!(first.len(), 2);
        assert_eq!(second.len(), 2);
        assert!(
            first.iter().all(|id| !second.contains(id)),
            "the second round repeated the first: {first:?} then {second:?}",
        );
    }

    /// A round that considered nothing must not throw the rotation away.
    ///
    /// `drain_ready` returns before looking at a single candidate when the destination is
    /// already at its window, which for a destination with a backlog is almost every tick. If
    /// that round reset the cursor, the next one would restart at the lowest chain id and the
    /// tail of the backlog would never be reached — the starvation the cursor exists to prevent,
    /// reintroduced by the cursor's own bookkeeping.
    #[test]
    fn a_round_that_visits_nothing_keeps_its_place() {
        let mut dest = test_dest_state();
        let ids = test_chain_ids(6);
        dest.lagging = ids.iter().copied().collect();

        let first = dest.drain_candidates(2);
        dest.advance_cursor(first.last().copied());
        let parked = dest.lagging_cursor;
        assert!(parked.is_some(), "the first round must park the cursor");

        // The saturated round: no candidate is visited, so nothing was considered.
        dest.advance_cursor(None);

        assert_eq!(
            dest.lagging_cursor, parked,
            "a round that visited nothing moved the cursor",
        );
        let resumed = dest.drain_candidates(2);
        assert!(
            resumed.iter().all(|id| !first.contains(id)),
            "the drain restarted at the front of the backlog: {first:?} then {resumed:?}",
        );
    }

    /// A peer that alternates advancing and regressing its reported height pays a doubling
    /// penalty, while one that genuinely restored pays a single delay.
    ///
    /// The first attempt at this backed a regression off using the shared `failures` counter,
    /// which an advance resets — so alternating answers pinned the delay at the base value
    /// forever and the "throttled" peer kept a pair re-reading its whole catch-up window at
    /// roughly two sends a second.
    #[test]
    fn an_oscillating_peer_escalates_but_a_restored_one_does_not() {
        let config = BlockExportConfig {
            retry_delay: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(60),
            ..BlockExportConfig::default()
        };
        let now = Timestamp::now();
        let tip = BlockHeight(100);

        // A genuine restore: one regression, then it advances from there for good.
        let mut restored = ChainDest {
            next_height: Some(BlockHeight(50)),
            ..ChainDest::default()
        };
        assert!(restored
            .record_reached(BlockHeight(10), tip, now, &config)
            .is_some());
        let restore_penalty = restored.retry_at.expect("a regression backs the pair off");
        assert_eq!(
            restore_penalty,
            now.saturating_add(TimeDelta::from_millis(100))
        );
        restored.record_reached(BlockHeight(20), tip, now, &config);
        assert_eq!(
            restored.retry_at, None,
            "an advance after the restore must clear the penalty",
        );

        // An oscillator: every regression costs double the last, however many advances it
        // interleaves.
        let mut liar = ChainDest {
            next_height: Some(BlockHeight(50)),
            ..ChainDest::default()
        };
        let mut penalties = Vec::new();
        for round in 0..4 {
            liar.record_reached(BlockHeight(10), tip, now, &config);
            penalties.push(
                liar.retry_at
                    .expect("a regression backs the pair off")
                    .delta_since(now),
            );
            liar.record_reached(BlockHeight(50 + round), tip, now, &config);
        }
        assert_eq!(
            penalties,
            vec![
                TimeDelta::from_millis(100),
                TimeDelta::from_millis(200),
                TimeDelta::from_millis(400),
                TimeDelta::from_millis(800),
            ],
            "an advance between regressions reset the penalty",
        );
    }

    /// A destination claiming a height above our own tip is acknowledged only up to that tip.
    ///
    /// The acknowledged height is merged into the chain's persisted `exported_heights` by
    /// maximum, so believing an over-report would write a "converged" marker that outlives the
    /// process and silently ends export to that pair. Being *ahead* is normal — the client
    /// broadcasts to everyone — so the report cannot simply be rejected either.
    #[test]
    fn a_height_above_our_tip_is_acknowledged_only_up_to_it() {
        let config = BlockExportConfig::default();
        let tip = BlockHeight(100);

        // An over-report is clamped — in the acknowledgement AND in the stored cursor. Leaving
        // the cursor raw satisfies every "converged" predicate and no "behind" one, and only a
        // completed send can rewrite it, so the pair would never be scheduled again.
        let mut liar = ChainDest::default();
        let acked = liar.record_reached(BlockHeight(u64::MAX), tip, Timestamp::now(), &config);
        assert_eq!(
            acked,
            Some(BlockHeight(99)),
            "acknowledged a height we never exported",
        );
        assert_eq!(
            liar.next_height,
            Some(tip),
            "stored a cursor above our tip: the pair is now unschedulable and reads as converged",
        );

        // And an honest report below the tip is taken at its word, not rounded up to it —
        // otherwise "always acknowledge tip - 1" would satisfy the clamp just as well.
        let mut honest = ChainDest::default();
        let acked = honest.record_reached(BlockHeight(40), tip, Timestamp::now(), &config);
        assert_eq!(
            acked,
            Some(BlockHeight(39)),
            "acknowledged more than the destination reported",
        );
        assert_eq!(honest.next_height, Some(BlockHeight(40)));
    }

    /// The regression counter has to be free: this is per (chain, destination), and a down peer
    /// holds one per tracked chain.
    #[test]
    fn the_regression_counter_costs_no_memory() {
        assert_eq!(size_of::<ChainDest>(), 56);
    }

    /// Our own storage failing is not the destination's fault, and not one pair's problem
    /// either: it shrinks the queue-wide budget, leaving the peer's window alone.
    ///
    /// Classifying it per-pair (as `ChainScoped` did) backs off one pair at a time while every
    /// other pair keeps reading at full rate, so the control loop cannot see the bottleneck it
    /// is creating.
    #[test]
    fn local_storage_errors_are_neither_chain_nor_destination_scoped() {
        let view_error = chain_client::Error::ViewError(linera_views::ViewError::NotFound(
            "storage is unhappy".to_owned(),
        ));
        assert!(is_local_scoped(&view_error));
        assert!(
            !is_chain_scoped(&view_error),
            "a storage failure would back off one pair and leave the budget untouched",
        );

        // A destination genuinely lacking the chain's events stays chain-scoped.
        let events_missing =
            chain_client::Error::RemoteNodeError(NodeError::EventsNotFound(vec![]));
        assert!(is_chain_scoped(&events_missing));
        assert!(!is_local_scoped(&events_missing));
    }

    /// The queue-wide budget is offered to a different destination each round.
    ///
    /// It is shared across destinations, so serving them in index order every time lets the
    /// first `max_in_flight_total / max_in_flight_per_destination` of them absorb all of it —
    /// with the defaults that is eight, and every destination past the eighth in a larger
    /// committee would get no catch-up at all.
    #[test]
    fn the_budget_is_offered_to_a_different_destination_each_round() {
        let indices = (0..12 as DestIndex).collect::<Vec<_>>();
        let mut cursor = None;
        let mut first_served = Vec::new();

        for _ in 0..12 {
            let order = rotated_order(&indices, cursor);
            first_served.push(order[0]);
            cursor = next_drain_cursor(&indices, order.first().copied());
        }

        assert_eq!(
            first_served,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            "the same destinations kept first claim on the budget",
        );
    }

    /// A drained burst hands back its peak-sized table for freeing off the mutex; a live one
    /// keeps its table.
    ///
    /// Pinned because the sweep's mutex-hold budget has been broken twice: once by never
    /// shrinking at all, once by an in-place shrink whose free of the peak table ran under the
    /// lock.
    #[test]
    fn forgetting_a_drained_burst_returns_its_table() {
        let mut progress = ProgressMap::default();
        let chains = test_chain_ids(20_000);
        for chain_id in &chains {
            progress.heights.insert(*chain_id, Vec::new());
        }
        let peak_capacity = progress.heights.capacity();

        // Still holding more than one sweep's budget: no rebuild, however oversized the table.
        // This is the case that separates the two halves of the guard — the table below is
        // several times too big, so a capacity test on its own would rebuild it here, under the
        // mutex, at a size the sweep's budget was written to exclude.
        let keep = MAX_FORGET_PER_SWEEP + 1000;
        let (bulk, _) = chains.split_at(chains.len() - keep);
        assert!(
            progress.forget_chains(bulk).is_none(),
            "rebuilt a map still holding {} entries, past the {MAX_FORGET_PER_SWEEP} budget",
            progress.heights.len(),
        );
        assert!(
            progress.heights.capacity() > progress.heights.len().saturating_mul(4),
            "the table has to be oversized here or the case proves nothing",
        );

        // Down to within the budget: the peak table is handed back, not freed in place.
        let survivors = 10;
        let within_budget = chains.len() - survivors;
        let old_table = progress.forget_chains(&chains[chains.len() - keep..within_budget]);
        assert!(
            old_table.is_some_and(|table| table.capacity() > peak_capacity / 2),
            "the peak-sized table was not handed back for freeing off the mutex",
        );
        assert!(progress.heights.capacity() < peak_capacity / 4);
        assert_eq!(progress.heights.len(), survivors);
    }

    /// Chain ids in the order `lagging` holds them, so a test can name "the front" of a backlog.
    fn test_chain_ids(count: usize) -> Vec<ChainId> {
        let mut ids = (0..count)
            .map(|i| ChainId(CryptoHash::test_hash(format!("chain{i}"))))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    /// Destinations indexed the way `sync_destinations` assigns them: in registration order.
    fn test_destinations(
        validators: impl IntoIterator<Item = ValidatorPublicKey>,
    ) -> BTreeMap<DestIndex, DestState<()>> {
        validators
            .into_iter()
            .enumerate()
            .map(|(index, validator)| {
                let dest = DestState {
                    validator,
                    ..test_dest_state()
                };
                (index as DestIndex, dest)
            })
            .collect()
    }

    fn test_dest_state() -> DestState<()> {
        DestState {
            node: (),
            validator: ValidatorPublicKey::test_key(0),
            address: "grpc:localhost:1".to_string(),
            generation: 1,
            in_flight: 0,
            window: 1,
            retry_at: None,
            failures: 0,
            lagging: BTreeSet::new(),
            lagging_cursor: None,
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
        let now = Timestamp::now();
        let mut delays = Vec::new();
        for _ in 0..4 {
            back_off(&mut failures, &mut retry_at, now, &config);
            delays.push(retry_at.expect("set by back_off").delta_since(now));
        }
        assert_eq!(
            delays,
            [
                TimeDelta::from_millis(100),
                TimeDelta::from_millis(200),
                TimeDelta::from_millis(400),
                TimeDelta::from_millis(450),
            ],
        );
    }
}
