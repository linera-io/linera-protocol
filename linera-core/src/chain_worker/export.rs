// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pushing executed blocks to the other validators in the committee.
//!
//! Each chain worker owns one export task. When the worker executes a block it hands the
//! certificate and the block's blobs — both already in memory — to that task, which pushes them to
//! every other validator in the chain's current committee. This is the validator-to-validator
//! dissemination that makes each validator a complete replica: consensus itself only guarantees
//! that a quorum holds any given block.
//!
//! One task per chain worker rather than one per process: thousands of chains through a single
//! task would serialize them, and a per-chain task gets per-chain height ordering for free, since
//! it finishes one block before taking the next off its queue.
//!
//! The queue is unbounded. Dropping a block would leave a gap in the destination that nothing ever
//! repairs, which is the failure mode this design exists to remove; a queue that grows is a
//! performance problem instead, and [`metrics::QUEUE_SIZE`] is there to see it.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

use futures::future;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlockHeight},
    identifiers::ChainId,
    time::{Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::committee::Committee;
use linera_storage::{Arc as CacheArc, Storage};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use crate::{
    local_node::LocalNodeClient,
    node::{ValidatorNode, ValidatorNodeProvider},
    remote_node::RemoteNode,
    updater::RemoteNodeUpdater,
};

#[cfg(with_metrics)]
mod metrics {
    use std::sync::LazyLock;

    use linera_base::prometheus_util::{
        exponential_bucket_interval, exponential_bucket_latencies, register_histogram,
        register_histogram_vec, register_int_gauge,
    };
    use prometheus::{Histogram, HistogramVec, IntGauge};

    /// Blocks waiting to be exported, across every chain worker in this process.
    ///
    /// The queue is unbounded, so this is how a chain that produces blocks faster than they can be
    /// pushed becomes visible instead of silently losing them.
    pub static QUEUE_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "block_export_queue_size",
            "Blocks queued for export across all chain workers",
        )
    });

    /// Time from a block being queued to it having been pushed to every destination.
    ///
    /// This is the export cost per block, including the serialization and signature work that
    /// shares a runtime thread with block execution on a single-threaded shard.
    pub static EXPORT_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
        register_histogram(
            "block_export_latency",
            "Time (ms) to export one block to every destination, including time spent queued",
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
    /// How many certificates are read from storage and pushed per batch when catching a
    /// destination up.
    pub certificate_upload_batch_size: u64,
    /// How long a destination is skipped after a failed push, doubling up to `max_retry_delay`
    /// while it keeps failing.
    ///
    /// A destination that is down must not stall export for the rest of the committee, and must
    /// not cost a full round trip on every block either. Skipping it leaves its cursor stale, so
    /// the first push that does go through queries it and fills in whatever it missed.
    ///
    /// This is a coarser layer than the transport's own per-request retries: those decide whether
    /// one call is worth attempting again, this decides whether the destination is worth
    /// attempting at all right now.
    pub retry_delay: Duration,
    /// The longest a failing destination is skipped for.
    pub max_retry_delay: Duration,
}

impl Default for BlockExportConfig {
    fn default() -> Self {
        BlockExportConfig {
            certificate_upload_batch_size: crate::client::DEFAULT_CERTIFICATE_UPLOAD_BATCH_SIZE,
            retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
        }
    }
}

/// Everything the export task needs that only the chain worker can supply.
pub struct ChainExportSetup<S: Storage> {
    /// The chain whose blocks this task exports.
    pub chain_id: ChainId,
    /// Read access to the chains this process serves, used for the few chain-level queries that
    /// the certificate partitions cannot answer — the admin chain's height, and block hashes for
    /// certificates that predate the height index.
    pub local_node: LocalNodeClient<S>,
    /// The heights already exported to each validator, as last persisted by the worker.
    pub exported_heights: BTreeMap<ValidatorPublicKey, BlockHeight>,
}

/// Creates the export task for one chain worker.
///
/// The transport is type-erased behind this alias: `linera-core` knows how to export a block, but
/// only the server binary knows how to reach another validator, so it installs a factory that
/// closes over its node provider.
pub type ChainExporterFactory<S> = Arc<dyn Fn(ChainExportSetup<S>) -> ChainExporter + Send + Sync>;

/// A block a chain worker has executed, on its way to the other validators.
struct ExportedBlock {
    certificate: CacheArc<ConfirmedBlockCertificate>,
    /// The block's required blobs, so that a destination missing them — which is always the case
    /// for a blob this very block publishes — is served without a read from storage. Held as the
    /// storage cache's pointers, so queued blocks share the allocations rather than copying them.
    blobs: Vec<CacheArc<Blob>>,
    /// The chain's committee *after* the block was applied, so that a validator joining in this
    /// block is exported to immediately, including the admin-chain block that admitted it.
    committee: Arc<Committee>,
    /// The chain carrying the epoch events, needed when a destination does not yet know the
    /// committee that signed the certificate.
    admin_chain_id: ChainId,
}

/// The chain worker's end of its export task.
pub struct ChainExporter {
    blocks: mpsc::UnboundedSender<ExportedBlock>,
    /// The highest height each validator has acknowledged, written by the export task and folded
    /// into the chain's `exported_heights` by the worker when it next saves.
    ///
    /// Progress travels this way rather than back through the worker so that the worker stays the
    /// only writer of its own chain state, and so that export costs no extra database write.
    progress: Arc<Mutex<BTreeMap<ValidatorPublicKey, BlockHeight>>>,
}

impl ChainExporter {
    /// Queues a block for export, and returns without waiting for it to be sent.
    ///
    /// Never blocks and never drops: see the module documentation for why the queue is unbounded.
    pub(crate) fn export(
        &self,
        certificate: CacheArc<ConfirmedBlockCertificate>,
        blobs: Vec<CacheArc<Blob>>,
        committee: Arc<Committee>,
        admin_chain_id: ChainId,
    ) {
        let block = ExportedBlock {
            certificate,
            blobs,
            committee,
            admin_chain_id,
        };
        match self.blocks.send(block) {
            Ok(()) => {
                #[cfg(with_metrics)]
                metrics::QUEUE_SIZE.inc();
            }
            // The task runs until this sender is dropped, so it cannot be gone while we hold it.
            Err(_) => {
                warn!("Block export task stopped unexpectedly; blocks are no longer being exported")
            }
        }
    }

    /// Returns how far each validator has been exported to, restricted to `committee` so that
    /// validators which have left it are pruned.
    pub(crate) fn progress(
        &self,
        committee: &Committee,
    ) -> BTreeMap<ValidatorPublicKey, BlockHeight> {
        let progress = self
            .progress
            .lock()
            .expect("progress mutex is never poisoned");
        progress
            .iter()
            .filter(|(validator, _)| committee.validators().contains_key(*validator))
            .map(|(validator, height)| (*validator, *height))
            .collect()
    }
}

/// Spawns the export task for one chain worker and returns the worker's end of it.
///
/// The task runs until the returned [`ChainExporter`] is dropped, which happens when the chain
/// worker is dropped — so an idle chain costs neither a task nor a connection.
pub fn spawn_chain_exporter<S, P>(
    setup: ChainExportSetup<S>,
    node_provider: Arc<P>,
    config: BlockExportConfig,
    own_public_key: Option<ValidatorPublicKey>,
) -> ChainExporter
where
    S: Storage + Clone + Send + Sync + 'static,
    P: ValidatorNodeProvider + Send + Sync + 'static,
    P::Node: Send + Sync,
{
    let (blocks, receiver) = mpsc::unbounded_channel();
    // Seed the progress map with what the worker had already persisted, so that the first fold
    // back into the chain state cannot regress it to nothing while the task has yet to send
    // anything.
    let progress = Arc::new(Mutex::new(setup.exported_heights.clone()));

    let task = ChainExportTask {
        chain_id: setup.chain_id,
        node_provider,
        local_node: setup.local_node,
        config,
        own_public_key,
        destinations: HashMap::new(),
        progress: progress.clone(),
    };
    linera_base::Task::spawn(task.run(receiver)).forget();

    ChainExporter { blocks, progress }
}

/// Consecutive failed pushes after which a destination's connection is rebuilt.
///
/// The transport may be relaying through one of several proxies, and a destination keeps whichever
/// it was given. If that proxy dies, this destination would otherwise keep failing against it
/// forever while the others stay healthy, so after a few failures the node is remade — which draws
/// a fresh proxy from the rotation. The failure count and backoff are deliberately *not* reset, so
/// a destination that is genuinely down still backs off rather than being hammered once per proxy.
const REBUILD_AFTER_FAILURES: u32 = 3;

/// One destination validator, and what we believe it holds of this chain.
struct Destination<S: Storage, N> {
    updater: RemoteNodeUpdater<S, N>,
    address: String,
    /// The next height the validator needs, or `None` when we have to ask it — before the first
    /// push, and after any failed one.
    next_height: Option<BlockHeight>,
    /// While this is in the future the validator is skipped; it failed recently.
    retry_at: Option<Instant>,
    /// How many pushes to this destination have failed in a row, which sets how long the next
    /// skip lasts.
    failures: u32,
}

/// The body of one chain's export task.
struct ChainExportTask<S, P>
where
    S: Storage,
    P: ValidatorNodeProvider,
{
    chain_id: ChainId,
    node_provider: Arc<P>,
    /// Answers the chain-level queries the certificate partitions cannot, through the local
    /// worker that owns the chain state views.
    local_node: LocalNodeClient<S>,
    config: BlockExportConfig,
    own_public_key: Option<ValidatorPublicKey>,
    destinations: HashMap<ValidatorPublicKey, Destination<S, P::Node>>,
    /// The highest height each validator has acknowledged. Seeded from what the worker had
    /// persisted, so a destination we meet for the first time — after a restart, or after the
    /// chain worker was dropped and reloaded — starts from there instead of being queried.
    progress: Arc<Mutex<BTreeMap<ValidatorPublicKey, BlockHeight>>>,
}

impl<S, P> ChainExportTask<S, P>
where
    S: Storage + Clone + 'static,
    P: ValidatorNodeProvider,
    P::Node: Clone + 'static,
{
    /// Exports each block in turn, and returns when the chain worker has dropped its end.
    #[instrument(level = "debug", skip_all, fields(chain_id = %self.chain_id))]
    async fn run(mut self, mut receiver: mpsc::UnboundedReceiver<ExportedBlock>) {
        while let Some(block) = receiver.recv().await {
            #[cfg(with_metrics)]
            metrics::QUEUE_SIZE.dec();
            #[cfg(with_metrics)]
            let _latency = metrics::EXPORT_LATENCY.measure_latency();
            self.export(block).await;
        }
        debug!("Chain worker dropped; stopping block export");
    }

    /// Pushes one block to every destination, concurrently, and records how far each got.
    async fn export(&mut self, block: ExportedBlock) {
        self.sync_destinations(&block);

        // Push to every destination concurrently, but only move on to the next block once they
        // have all answered: that is what gives each destination the blocks of this chain in
        // height order.
        let now = Instant::now();
        let (config, block) = (&self.config, &block);
        let results = future::join_all(self.destinations.iter_mut().map(
            |(validator, destination)| async move {
                (*validator, destination.send(block, now, config).await)
            },
        ))
        .await;

        // `next_height` is what the validator itself reported, so the recorded height is a fact
        // rather than an assumption: a validator that could not close its gap reports the truth
        // and keeps a low cursor here, and is queried again on the next block.
        let mut progress = self
            .progress
            .lock()
            .expect("progress mutex is never poisoned");
        for (validator, next_height) in results {
            if let Some(Ok(height)) = next_height.map(BlockHeight::try_sub_one) {
                progress.insert(validator, height);
            }
        }
    }

    /// Brings the destination set in line with the block's committee: adds validators that joined,
    /// drops those that left, and re-creates a node whose address changed.
    fn sync_destinations(&mut self, block: &ExportedBlock) {
        let committee = &block.committee;
        self.destinations.retain(|validator, destination| {
            committee
                .validators()
                .get(validator)
                .is_some_and(|state| state.network_address == destination.address)
        });

        // A destination that has failed repeatedly may be stuck behind a dead proxy, so rebuild
        // its node alongside any validator we do not have yet.
        let stuck = self
            .destinations
            .iter()
            .filter(|(_, destination)| destination.failures >= REBUILD_AFTER_FAILURES)
            .map(|(validator, _)| *validator)
            .collect::<Vec<_>>();

        let missing = committee
            .validator_addresses()
            .filter(|(validator, _)| {
                Some(*validator) != self.own_public_key
                    && (!self.destinations.contains_key(validator) || stuck.contains(validator))
            })
            .map(|(validator, address)| (validator, address.to_owned()))
            .collect::<Vec<_>>();
        if missing.is_empty() {
            return;
        }

        let nodes = match self.node_provider.make_nodes_from_list(missing) {
            Ok(nodes) => nodes.collect::<Vec<_>>(),
            Err(error) => {
                // Leaves the destinations we already have untouched, and tries again on the next
                // block; a committee we cannot resolve at all is a configuration problem.
                warn!(%error, "Cannot reach the committee to export blocks to");
                return;
            }
        };

        let acknowledged = self
            .progress
            .lock()
            .expect("progress mutex is never poisoned")
            .clone();
        for (validator, node) in nodes {
            let Some(address) = committee
                .validators()
                .get(&validator)
                .map(|state| state.network_address.clone())
            else {
                continue;
            };
            let updater = RemoteNodeUpdater {
                remote_node: RemoteNode {
                    public_key: validator,
                    node,
                },
                local_node: self.local_node.clone(),
                admin_chain_id: block.admin_chain_id,
                certificate_upload_batch_size: self.config.certificate_upload_batch_size,
            };
            // Rebuilding a stuck destination keeps its failure count and retry time: only the
            // connection is replaced, so a validator that is itself down keeps backing off.
            let previous = self.destinations.remove(&validator);
            self.destinations.insert(
                validator,
                Destination {
                    updater,
                    address,
                    // An acknowledged height was reported by the validator itself, so the block
                    // after it is the one the validator needs next.
                    next_height: acknowledged
                        .get(&validator)
                        .map(|height| BlockHeight(height.0.saturating_add(1))),
                    retry_at: previous.as_ref().and_then(|d| d.retry_at),
                    failures: previous.map_or(0, |d| d.failures),
                },
            );
        }
    }
}

impl<S, N> Destination<S, N>
where
    S: Storage + Clone + 'static,
    N: ValidatorNode + Clone + 'static,
{
    /// Pushes the block, along with any earlier ones this validator is missing, and returns the
    /// validator's next block height afterwards — or `None` if it was skipped or failed.
    async fn send(
        &mut self,
        block: &ExportedBlock,
        now: Instant,
        config: &BlockExportConfig,
    ) -> Option<BlockHeight> {
        if self.retry_at.is_some_and(|retry_at| now < retry_at) {
            return None;
        }

        #[cfg(with_metrics)]
        let send_latency = metrics::SEND_LATENCY.with_label_values(&[&self.address]);
        #[cfg(with_metrics)]
        let _latency = send_latency.measure_latency();
        #[cfg(with_metrics)]
        metrics::DESTINATION_LAG
            .with_label_values(&[&self.address])
            .observe(self.lag(block) as f64);

        match self
            .updater
            .send_block(&block.certificate, &block.blobs, self.next_height)
            .await
        {
            Ok(next_height) => {
                self.next_height = Some(next_height);
                self.retry_at = None;
                self.failures = 0;
                Some(next_height)
            }
            Err(error) => {
                let backoff = config
                    .retry_delay
                    .saturating_mul(1u32.checked_shl(self.failures).unwrap_or(u32::MAX))
                    .min(config.max_retry_delay);
                let height = block.certificate.block().header.height;
                warn!(
                    validator = %self.address, %height, %error, ?backoff,
                    "Failed to export block; will query the validator before the next push",
                );
                // Forget the cursor: the next push must ask what this validator actually has
                // rather than assume the block we just failed to send arrived.
                self.next_height = None;
                self.retry_at = Some(now + backoff);
                self.failures = self.failures.saturating_add(1);
                None
            }
        }
    }

    /// How many blocks this validator is believed to be missing before the block being pushed.
    #[cfg(with_metrics)]
    fn lag(&self, block: &ExportedBlock) -> u64 {
        let height = block.certificate.block().header.height;
        match self.next_height {
            Some(next_height) => height.0.saturating_sub(next_height.0),
            // We have no cursor, so we are about to query; report the gap as unknown-but-nonzero.
            None => 1,
        }
    }
}
