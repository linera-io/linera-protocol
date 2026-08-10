// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Pushing executed blocks to the other validators in the committee.
//!
//! Each chain worker owns one export task and hands it every block it executes — certificate and
//! blobs, both already in memory. This is the dissemination that makes each validator a complete
//! replica; consensus alone only guarantees that a quorum holds any given block.
//!
//! One task per chain worker rather than per process: a single task would serialize thousands of
//! chains, and a per-chain task gets height ordering for free.
//!
//! The queue is unbounded, because dropping a block would leave a gap nothing repairs — the very
//! failure this design removes. A growing queue is a performance problem instead; see
//! [`metrics::QUEUE_SIZE`].

use std::{
    collections::{BTreeMap, HashMap},
    iter,
    sync::{Arc, Mutex},
};

use futures::future;
#[cfg(with_metrics)]
use linera_base::prometheus_util::MeasureLatency as _;
use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, BlockHeight},
    identifiers::ChainId,
    time::{timer::timeout, Duration, Instant},
};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_execution::committee::Committee;
use linera_storage::{Arc as CacheArc, Storage};
use tokio::sync::mpsc;
use tracing::{debug, instrument, warn};

use crate::{
    data_types::ChainInfoQuery,
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

    /// Blocks waiting to be exported, across every chain worker in this process. The queue is
    /// unbounded, so this is how a chain producing faster than it can push becomes visible.
    pub static QUEUE_SIZE: LazyLock<IntGauge> = LazyLock::new(|| {
        register_int_gauge(
            "block_export_queue_size",
            "Blocks queued for export across all chain workers",
        )
    });

    /// Time from a block being queued to it having been pushed to every destination.
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
    /// How long a destination is skipped after a failed push, doubling up to `max_retry_delay`.
    /// Coarser than the transport's per-request retries: those decide whether one call is worth
    /// repeating, this decides whether the destination is worth attempting at all right now.
    pub retry_delay: Duration,
    /// The longest a failing destination is skipped for.
    pub max_retry_delay: Duration,
    /// How long the task waits for a new block before spending a round catching up destinations
    /// that are behind. With `max_catch_up_blocks` this sets the backfill rate, so tune them
    /// together.
    pub idle_catch_up_interval: Duration,
    /// How many missing blocks are pushed to one destination per round. Deliberately small: it
    /// bounds what a live block may wait behind, and a validator that just joined reports height
    /// 0, so its catch-up is otherwise arbitrarily large.
    pub max_catch_up_blocks: u64,
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
        Ok(())
    }
}

impl Default for BlockExportConfig {
    fn default() -> Self {
        BlockExportConfig {
            certificate_upload_batch_size: crate::client::DEFAULT_CERTIFICATE_UPLOAD_BATCH_SIZE,
            retry_delay: Duration::from_secs(1),
            max_retry_delay: Duration::from_secs(60),
            idle_catch_up_interval: Duration::from_millis(200),
            max_catch_up_blocks: 200,
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

/// Creates the export task for one chain worker. Type-erased because only the server binary knows
/// how to reach another validator.
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
    /// When the worker queued this block, so that `EXPORT_LATENCY` covers the wait as well as
    /// the sending. The queue is where the delay shows up once export falls behind, which is
    /// exactly when the metric is worth reading.
    #[cfg(with_metrics)]
    queued_at: Instant,
}

/// The chain worker's end of its export task.
pub struct ChainExporter {
    blocks: mpsc::UnboundedSender<ExportedBlock>,
    /// The highest height each validator has acknowledged, folded into the chain's
    /// `exported_heights` by the worker on its next save. Travels this way so the worker stays the
    /// only writer of its own chain state, and export costs no extra database write.
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
            #[cfg(with_metrics)]
            queued_at: Instant::now(),
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

/// Spawns the export task for one chain worker and returns the worker's end of it. The task runs
/// until the returned [`ChainExporter`] is dropped with its worker, so an idle chain costs neither
/// a task nor a connection.
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
        chain_next_height: BlockHeight::ZERO,
        admin_chain_id: None,
        progress: progress.clone(),
    };
    linera_base::Task::spawn(task.run(receiver)).forget();

    ChainExporter { blocks, progress }
}

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
    /// The height after the last block handed to this task: how far a destination must reach to
    /// be considered caught up. Used to keep catching a lagging destination up while the chain is
    /// idle, so that convergence does not depend on the chain producing more blocks.
    chain_next_height: BlockHeight,
    /// The chain carrying the epoch events, learned from the first block exported. `None` until
    /// then, which is also when there is nothing to catch anyone up on.
    admin_chain_id: Option<ChainId>,
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
        loop {
            match timeout(self.config.idle_catch_up_interval, receiver.recv()).await {
                Ok(Some(block)) => {
                    #[cfg(with_metrics)]
                    metrics::QUEUE_SIZE.dec();
                    #[cfg(with_metrics)]
                    let queued_at = block.queued_at;
                    self.export(block).await;
                    #[cfg(with_metrics)]
                    metrics::EXPORT_LATENCY
                        .finish_measurement(queued_at.elapsed().as_secs_f64() * 1000.0);
                }
                // The chain worker dropped its end.
                Ok(None) => break,
                // Nothing arrived for a while: close more of a lagging destination's gap rather
                // than waiting for a block the chain may never produce. One round per interval,
                // so a destination that cannot be caught up trickles rather than spins.
                Err(_) => self.catch_up_round().await,
            }
        }
        debug!("Chain worker dropped; stopping block export");
    }

    /// Brings the destination set in line with the committee as the worker sees it now. Asked of
    /// the worker rather than read from the chain view, which the worker owns while running.
    async fn sync_destinations_from_local_node(&mut self) {
        let query = ChainInfoQuery::new(self.chain_id).with_committees();
        let info = match self.local_node.handle_chain_info_query(query).await {
            Ok(response) => response.info,
            Err(error) => {
                debug!(%error, "Cannot read the committee while catching up");
                return;
            }
        };
        let Some(committee) = info
            .requested_committees
            .and_then(|committees| committees.into_iter().next_back().map(|(_, c)| c))
        else {
            return;
        };
        let Some(admin_chain_id) = self.admin_chain_id else {
            return;
        };
        self.sync_destinations_with(&committee, admin_chain_id);
    }

    /// Remakes the node of every destination whose last push failed, drawing the next proxy from
    /// the rotation so a dead proxy is stepped over rather than retried. Keeps the failure count
    /// and backoff, so this cannot become a way to hammer a validator that is simply offline.
    fn refresh_stuck_destinations(&mut self) {
        let stuck = self
            .destinations
            .iter()
            .filter(|(_, destination)| destination.failures > 0)
            .map(|(validator, destination)| (*validator, destination.address.clone()))
            .collect::<Vec<_>>();
        if stuck.is_empty() {
            return;
        }
        let nodes = match self.node_provider.make_nodes_from_list(stuck) {
            Ok(nodes) => nodes.collect::<Vec<_>>(),
            Err(error) => {
                warn!(%error, "Cannot rebuild connections to lagging validators");
                return;
            }
        };
        for (validator, node) in nodes {
            if let Some(destination) = self.destinations.get_mut(&validator) {
                destination.updater.remote_node.node = node;
            }
        }
    }

    /// Pushes one bounded chunk of missing blocks to every destination that is behind.
    ///
    /// Runs only when nothing is queued, so live blocks always take priority over backfill.
    async fn catch_up_round(&mut self) {
        // Pick up committee changes too. A validator admitted while this chain happens to be idle
        // would otherwise never become a destination at all — `sync_destinations` runs off an
        // exported block, and there is no next block to carry the new committee.
        self.sync_destinations_from_local_node().await;
        self.refresh_stuck_destinations();
        let now = Instant::now();
        let (config, chain_id, target) = (&self.config, self.chain_id, self.chain_next_height);
        let results = future::join_all(
            self.destinations
                .iter_mut()
                .filter(|(_, destination)| destination.is_behind(target) && destination.is_due(now))
                .map(|(validator, destination)| async move {
                    (
                        *validator,
                        destination.catch_up(chain_id, target, now, config).await,
                    )
                }),
        )
        .await;
        self.record_progress(results);
    }

    /// Pushes one block to every destination, concurrently, and records how far each got.
    async fn export(&mut self, block: ExportedBlock) {
        self.chain_next_height = block
            .certificate
            .block()
            .header
            .height
            .try_add_one()
            .unwrap_or(BlockHeight::MAX);
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

        self.record_progress(results);
    }

    /// Records how far each destination acknowledged, for the worker to persist on its next save.
    /// These are heights the validators reported themselves, so one that could not close its gap
    /// keeps a low cursor and is picked up again next round.
    fn record_progress(&self, results: Vec<(ValidatorPublicKey, Option<BlockHeight>)>) {
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
        self.admin_chain_id = Some(block.admin_chain_id);
        self.sync_destinations_with(&block.committee, block.admin_chain_id);
    }

    /// Adds destinations for validators in `committee` we do not have, and drops those that have
    /// left it or changed address.
    fn sync_destinations_with(&mut self, committee: &Committee, admin_chain_id: ChainId) {
        self.destinations.retain(|validator, destination| {
            committee
                .validators()
                .get(validator)
                .is_some_and(|state| state.network_address == destination.address)
        });

        // A destination whose last push failed may be stuck behind a dead proxy, so rebuild its
        // node alongside any validator we do not have yet.
        let stuck = self
            .destinations
            .iter()
            .filter(|(_, destination)| destination.failures > 0)
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

        // One at a time: resolving as a batch fails the whole batch on the first bad address,
        // which would stop this chain exporting to *any* validator, on every later block.
        let nodes = missing
            .into_iter()
            .filter_map(|(validator, address)| {
                match self
                    .node_provider
                    .make_nodes_from_list(iter::once((validator, address.clone())))
                {
                    Ok(nodes) => nodes.into_iter().next(),
                    Err(error) => {
                        warn!(
                            %validator, %address, %error,
                            "Cannot reach a committee member to export blocks to; \
                             continuing with the others",
                        );
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

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
                admin_chain_id,
                certificate_upload_batch_size: self.config.certificate_upload_batch_size,
                // Export replicates blocks; it does not take part in the destination's consensus.
                sync_consensus_rounds: false,
                max_admin_catch_up_blocks: Some(self.config.max_catch_up_blocks),
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

        let height = block.certificate.block().header.height;
        let result = self
            .updater
            .send_block(
                &block.certificate,
                &block.blobs,
                self.next_height,
                config.max_catch_up_blocks,
            )
            .await;
        self.record_outcome(result, height, now, config)
    }

    /// Whether this destination is, as far as we know, below `target`. An unknown height counts
    /// as behind: that is what a failed send leaves, and assuming otherwise strands it silently.
    fn is_behind(&self, target: BlockHeight) -> bool {
        self.next_height.is_none_or(|next| next < target)
    }

    /// Whether this destination's backoff, if any, has expired.
    fn is_due(&self, now: Instant) -> bool {
        self.retry_at.is_none_or(|retry_at| retry_at <= now)
    }

    /// Pushes one bounded chunk of the blocks this validator is missing below `target`, so one
    /// that joined partway through a long chain converges without waiting for new blocks.
    async fn catch_up(
        &mut self,
        chain_id: ChainId,
        target: BlockHeight,
        now: Instant,
        config: &BlockExportConfig,
    ) -> Option<BlockHeight> {
        #[cfg(with_metrics)]
        let send_latency = metrics::SEND_LATENCY.with_label_values(&[&self.address]);
        #[cfg(with_metrics)]
        let _latency = send_latency.measure_latency();

        let previous = self.next_height;
        let result = self
            .updater
            .send_missing_blocks(
                chain_id,
                target,
                self.next_height,
                config.max_catch_up_blocks,
            )
            .await;
        let outcome = self.record_outcome(result, target, now, config);

        // A round can succeed and advance nothing, since a chain we merely receive from is
        // stored only at its message-bearing blocks; without backing off, a destination we can
        // never finish would spin the task. Success only: `record_outcome` handles failures.
        if let Some(reached) = outcome {
            let advanced = previous.is_none_or(|before| reached > before);
            if !advanced {
                self.back_off(now, config);
            }
        }
        outcome
    }

    /// Folds the outcome of a push into this destination's cursor and backoff.
    fn record_outcome(
        &mut self,
        result: Result<BlockHeight, crate::client::chain_client::Error>,
        height: BlockHeight,
        now: Instant,
        config: &BlockExportConfig,
    ) -> Option<BlockHeight> {
        match result {
            Ok(next_height) => {
                self.next_height = Some(next_height);
                self.retry_at = None;
                self.failures = 0;
                Some(next_height)
            }
            Err(error) => {
                warn!(
                    validator = %self.address, %height, %error,
                    "Failed to export block; will query the validator before the next push",
                );
                // Forget the cursor: the next push must ask what this validator actually has
                // rather than assume the block we just failed to send arrived.
                self.next_height = None;
                self.back_off(now, config);
                None
            }
        }
    }

    /// Holds this destination off for a growing interval, capped by the configured maximum.
    fn back_off(&mut self, now: Instant, config: &BlockExportConfig) {
        let backoff = config
            .retry_delay
            .saturating_mul(1u32.checked_shl(self.failures).unwrap_or(u32::MAX))
            .min(config.max_retry_delay);
        self.retry_at = Some(now + backoff);
        self.failures = self.failures.saturating_add(1);
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
