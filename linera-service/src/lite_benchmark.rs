// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

// `ClientMode::Full`'s deeply generic, deeply nested async call chain (Client<FullEnv> ->
// ChainClient -> RequestsScheduler -> ...) overflows the default recursion limit while the
// compiler proves the futures spawned in `run_chain` are `Send`. Not a real recursion --
// just more type-checking headroom than the default budget.
#![recursion_limit = "512"]

//! A lightweight benchmark client that spams transactions through real consensus without
//! running a full node.
//!
//! Unlike `linera-benchmark` (which drives every transaction through a `ChainClient`, and
//! therefore needs a full `Storage` backend and locally executes every block's WASM
//! application before submitting it), this client only does the minimum work required to
//! produce a validly-signed block proposal: it tracks the tip hash/height/epoch of each
//! chain itself, signs a fresh `BlockProposal` with no execution outcome attached, and lets
//! the validators compute and vote on the result. It does not verify validator signatures
//! (it trusts responses at face value and simply counts successes/failures) but it does
//! drive the real propose -> vote -> certificate -> commit cycle, so the validators see and
//! process the same consensus traffic they would from any other client.
//!
//! Every benchmarked chain must be owned by a single *super owner* (see
//! `linera open-chain --super-owner`), so that every block proposal is made in `Round::Fast`
//! and validators vote to confirm it directly, without the validate-then-confirm two-phase
//! exchange that other rounds require.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File},
    io::Write as _,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context as _, Result};
use clap::Parser;
use futures::future::join_all;
use linera_base::{
    crypto::{CryptoHash, InMemorySigner, Signer as _, ValidatorPublicKey, ValidatorSignature},
    data_types::{Epoch, Round, Timestamp},
    identifiers::{AccountOwner, ChainId},
};
use linera_cache::ValueCache;
use linera_chain::{
    data_types::{BlockProposal, IncomingBundle, ProposedBlock, Transaction},
    justification::JustificationChain,
    types::{
        CertificateKind, CertificateValue as _, ConfirmedBlock, ConfirmedBlockCertificate,
        GenericCertificate,
    },
};
use linera_client::benchmark::{NativeFungibleTransferGenerator, OperationGenerator};
use linera_core::{
    client::chain_client,
    data_types::{ChainInfoQuery, ClientOutcome},
    environment,
    node::{CrossChainMessageDelivery, ValidatorNode, ValidatorNodeProvider as _},
    remote_node::RemoteNode,
};
use linera_execution::{committee::Committee, Operation};
use linera_rpc::{node_provider::DEFAULT_MAX_BACKOFF, Client, NodeOptions, NodeProvider};
use linera_storage::{DbStorage, StorageCacheConfig, WallClock};
use linera_views::backends::memory::{MemoryDatabase, MemoryStoreConfig};
use linera_wallet_json::{Keystore, PersistentWallet};
use num_format::{Locale, ToFormattedString as _};
use rand::Rng as _;
use tokio::{task, time};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// The cross-chain traffic pattern to generate, mirroring `linera-paper-eval`'s
/// `TransferTargetMode`.
#[derive(Clone, Copy, Debug, clap::ValueEnum)]
enum TrafficMode {
    /// Every transfer is a self-transfer: no cross-chain messages at all.
    Independent,
    /// Transfers alternate between a self-transfer and a transfer to another benchmarked
    /// chain, chosen at random.
    Mixed,
    /// Every transfer goes to another benchmarked chain, chosen at random; never to self.
    Full,
}

/// Which client drives the benchmark: see the module doc comment for the RTT-count
/// difference between the two.
#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum)]
enum ClientMode {
    /// The default: no local storage or execution, 3 round trips per block.
    Lite,
    /// A real `ChainClient` backed by local in-memory storage: executes every block itself
    /// (native operations only -- no WASM runtime is set up), 2 round trips per block.
    /// Currently only supports `--traffic-mode independent`.
    Full,
}

/// The `Environment` for `ClientMode::Full`: a real, in-process `ChainClient`, backed by
/// local in-memory storage rather than nothing (`LiteChainClient`) or a remote validator's
/// storage. `NodeProvider` already implements `ValidatorNodeProvider`, which is all
/// `environment::Network` requires, so it's reused as-is from the lite client's own setup.
type FullEnv = environment::Impl<
    DbStorage<MemoryDatabase, WallClock>,
    NodeProvider,
    InMemorySigner,
    environment::wallet::Memory,
>;
type FullChainClient = chain_client::ChainClient<FullEnv>;

#[derive(clap::Parser)]
#[command(
    name = "linera-lite-benchmark",
    version = linera_version::VersionInfo::default_clap_str(),
    about = "Spam transactions through consensus with a minimal, storage-free client",
)]
struct Args {
    /// Path to the wallet file (for the genesis config / committee and chain ownership).
    #[arg(long)]
    wallet: PathBuf,

    /// Path to the keystore file (for the signing keys of the chains' super owners).
    #[arg(long)]
    keystore: PathBuf,

    /// The chains to benchmark. Each must be owned by a single super owner whose key is in
    /// the keystore. Defaults to every chain in the wallet that has an owner.
    #[arg(long, value_delimiter = ',')]
    chains: Vec<ChainId>,

    /// Target number of blocks per second, summed across all benchmarked chains. Ignored if
    /// --bps-schedule is given.
    #[arg(long, default_value = "1")]
    bps: usize,

    /// A time-varying target rate instead of a fixed --bps: comma-separated
    /// `offset_seconds:bps` pairs (e.g. "0:50,30:50,30:800,120:800,150:50"), each giving the
    /// total bps (summed across all benchmarked chains, evenly split) from that offset until
    /// the next one. Must start at offset 0. Re-evaluated a few times a second, so a rate
    /// change takes effect within roughly a tick of its offset, not instantly.
    #[arg(long)]
    bps_schedule: Option<String>,

    /// Number of operations to include in each block. Ignored if
    /// --transactions-per-block-file is given.
    #[arg(long, default_value = "1")]
    transactions_per_block: usize,

    /// Path to a file with one block size (number of transactions) per line, e.g. the
    /// per-block transaction counts of a real Ethereum trace. If given, every block's size is
    /// taken from this sequence instead of --transactions-per-block: each chain starts at its
    /// own offset (offsets are spread randomly over the sequence, so the chains don't all
    /// propose identically-sized blocks at the same time) and then walks it one entry per
    /// block, wrapping around forever. Blank lines and lines starting with `#` are ignored.
    #[arg(long)]
    transactions_per_block_file: Option<PathBuf>,

    /// Scaling factor applied to every entry of --transactions-per-block-file: 0.5 halves
    /// each block, 2.0 doubles it. Scaled sizes are rounded to the nearest integer and
    /// clamped to at least 1, since validators reject empty blocks.
    #[arg(long, default_value = "1.0")]
    transactions_per_block_scale: f64,

    /// The cross-chain traffic pattern: independent (self-transfers only, the default),
    /// mixed (half self, half spread across the other benchmarked chains), or full (always a
    /// different benchmarked chain). `mixed` and `full` require at least 2 chains (across
    /// --chains and --destination-chains).
    #[arg(long, value_enum, default_value_t = TrafficMode::Independent)]
    traffic_mode: TrafficMode,

    /// The maximum number of incoming message bundles to drain into each block, on top of its
    /// operations. In every mode except `independent`, each block first receives the messages
    /// waiting in its chain's inboxes (the cross-chain transfers other chains sent it) so the
    /// inboxes don't grow without bound; this caps how many bundles one block will absorb, so
    /// a backlog is drained over several blocks rather than in one huge, slow block. Defaults
    /// to twice that block's own operation count, which self-tunes with variable block sizes
    /// and drains a backlog at twice the arrival rate. No effect in `independent` mode, which
    /// never generates cross-chain messages.
    #[arg(long)]
    max_incoming_bundles_per_block: Option<usize>,

    /// In `mixed`/`full` traffic modes, still generate and send cross-chain messages as usual,
    /// but never drain a chain's own inboxes into its blocks -- isolates the CPU cost of
    /// *sending* messages (cross-chain routing/delivery) from the cost of *receiving* them
    /// (verifying and applying incoming bundles as `ReceiveMessages` transactions), since both
    /// are normally on at once whenever `process_messages` is true. Inboxes just grow unbounded
    /// for the run's duration; fine for a short benchmark, not a realistic steady state. No
    /// effect in `independent` mode, which never generates cross-chain messages either way.
    #[arg(long)]
    skip_message_processing: bool,

    /// Broadcast the confirmed certificate's compact, value-free form (hash + signatures) to
    /// each validator known to have voted for it, instead of the full certificate (which
    /// re-embeds the whole executed block). A validator that already voted has the value
    /// cached, so this only shrinks the third and final round trip's payload; a validator that
    /// fell behind and forgot the value transparently gets a retry with the full certificate
    /// (see `RemoteNode::handle_optimized_confirmed_certificate`). Off by default.
    #[arg(long)]
    light_certificates: bool,

    /// Which client drives the benchmark -- lite (default, 3 RTTs/block) or full (2
    /// RTTs/block, real local execution). See `ClientMode`'s doc comment.
    #[arg(long, value_enum, default_value_t = ClientMode::Lite)]
    client_mode: ClientMode,

    /// Send every transfer in a block to the same destination chain (chosen fresh each block),
    /// instead of letting each transfer pick its own destination. All of a block's cross-chain
    /// messages then land in one recipient's inbox as a run of separate single-message bundles.
    /// Only affects `mixed`/`full` traffic modes. Off by default: transfers spread across
    /// destinations.
    #[arg(long)]
    single_destination_per_block: bool,

    /// Extra chains to address mixed/full traffic-mode messages to, in addition to --chains,
    /// without giving them their own run_chain task -- no traffic ever originates from them,
    /// so they don't consume any of the actively-benchmarked chains' shard capacity. Useful
    /// for isolating a single actively-driven chain/worker while still exercising
    /// cross-chain sends; their inboxes are simply left unprocessed. Must already exist
    /// (e.g. via `linera benchmark single`) like any other chain.
    #[arg(long, value_delimiter = ',')]
    destination_chains: Vec<ChainId>,

    /// If set, stop after this many seconds.
    #[arg(long)]
    runtime_in_seconds: Option<u64>,

    /// If set, write one row per committed/failed block to this CSV path, with columns
    /// chain_id,ts_within_experiment,num_tx,num_bundles,result,duration_micros -- num_tx is
    /// the block's operation count and num_bundles the incoming message bundles it drained
    /// (see --max-incoming-bundles-per-block); a superset of `linera-paper-eval`'s
    /// `transfers.csv` schema.
    #[arg(long)]
    output_csv: Option<PathBuf>,

    /// The number of Tokio worker threads to use. Defaults to the number of CPUs, like a bare
    /// `#[tokio::main]` -- fine for a single client process, but this client is normally run
    /// many-to-a-machine (one OS process per --num-clients in run_lite_benchmark_parallel.sh),
    /// and each process's chain tasks are I/O-bound (waiting on the network), not CPU-bound, so
    /// they don't need a full core's worth of threads each. At high client counts on a modest
    /// machine, N processes defaulting to num_cpus threads apiece can oversubscribe the host by
    /// an order of magnitude, adding scheduling jitter that shows up as client-observed tail
    /// latency indistinguishable from real network/validator latency. Set this low (e.g. 2) when
    /// running many clients per machine.
    #[arg(long)]
    tokio_threads: Option<usize>,

    /// Adds this many milliseconds of artificial delay before every gRPC request to a
    /// validator, simulating a higher-latency (e.g. WAN) link than whatever the client and
    /// validator actually measure between them. 0 (default) adds no delay. Useful for testing
    /// whether a difference in round trips per block (e.g. --client-mode or
    /// --light-certificates) matters more once each round trip is expensive.
    #[arg(long, default_value_t = 0)]
    simulated_latency_ms: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut builder = if args.tokio_threads == Some(1) {
        tokio::runtime::Builder::new_current_thread()
    } else {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        if let Some(threads) = args.tokio_threads {
            builder.worker_threads(threads);
        }
        builder
    };
    builder
        .enable_all()
        .build()
        .context("failed to create the Tokio runtime")?
        .block_on(run(args))
}

async fn run(args: Args) -> Result<()> {
    linera_service::tracing::init("lite-benchmark");

    let wallet = PersistentWallet::read(&args.wallet).context("failed to read the wallet")?;
    let keystore = Keystore::read(&args.keystore).context("failed to read the keystore")?;
    let signer = keystore.into_signer();

    let committee = wallet.genesis_config().committee.clone();
    let node_provider = NodeProvider::new(NodeOptions {
        send_timeout: Duration::from_secs(4),
        recv_timeout: Duration::from_secs(4),
        retry_delay: Duration::from_millis(200),
        max_retries: 10,
        max_backoff: DEFAULT_MAX_BACKOFF,
        simulated_latency: Duration::from_millis(args.simulated_latency_ms),
    });
    let nodes: Vec<(ValidatorPublicKey, Client)> = node_provider
        .make_nodes(&committee)
        .context("failed to create validator node clients")?
        .collect();
    anyhow::ensure!(!nodes.is_empty(), "the committee has no validators");

    let chain_ids = if args.chains.is_empty() {
        wallet.owned_chain_ids()
    } else {
        args.chains.clone()
    };
    anyhow::ensure!(!chain_ids.is_empty(), "no chains to benchmark");

    // Destinations for mixed/full traffic modes: the actively-driven chains themselves, plus
    // any purely-passive destination chains from --destination-chains (see its doc comment).
    let mut all_chain_ids = chain_ids.clone();
    all_chain_ids.extend(args.destination_chains.iter().copied());
    anyhow::ensure!(
        all_chain_ids.len() > 1 || matches!(args.traffic_mode, TrafficMode::Independent),
        "traffic-mode {:?} requires at least 2 chains (across --chains and --destination-chains)",
        args.traffic_mode
    );

    // For ClientMode::Full, one Client<FullEnv> is shared by every chain this process drives:
    // it owns the local in-memory storage and the in-memory wallet those chains' ChainClients
    // read/write through. Built once, up front, rather than per chain.
    let full_client: Option<Arc<linera_core::client::Client<FullEnv>>> =
        if args.client_mode == ClientMode::Full {
            anyhow::ensure!(
                matches!(args.traffic_mode, TrafficMode::Independent),
                "--client-mode full currently only supports --traffic-mode independent"
            );
            let mut storage = DbStorage::<MemoryDatabase, WallClock>::maybe_create_and_connect(
                &MemoryStoreConfig { kill_on_drop: true },
                "lite-benchmark-full-client",
                None,
                StorageCacheConfig {
                    blob_cache_size: 1000,
                    confirmed_block_cache_size: 1000,
                    certificate_cache_size: 1000,
                    certificate_raw_cache_size: 1000,
                    event_cache_size: 1000,
                    block_hash_by_height_cache_size: 1000,
                    event_block_height_cache_size: 1000,
                    cache_cleanup_interval_secs: 3600,
                },
            )
            .await
            .context("failed to set up local in-memory storage for the full client")?;
            wallet
                .genesis_config()
                .initialize_storage(&mut storage)
                .await
                .context("failed to initialize local storage from genesis")?;
            let environment = environment::Impl {
                storage,
                network: node_provider.clone(),
                signer: signer.clone(),
                wallet: environment::wallet::Memory::default(),
            };
            let chain_client_options = chain_client::Options {
                max_pending_message_bundles: 10,
                max_block_limit_errors: 3,
                staging_bundles_time_budget: None,
                message_policy: Default::default(),
                priority_bundle_origins: HashSet::new(),
                cross_chain_message_delivery: CrossChainMessageDelivery::NonBlocking,
                quorum_grace_period: 0.1,
                blob_download_hedge_delay: Duration::from_secs(1),
                certificate_batch_download_hedge_delay: Duration::from_secs(1),
                certificate_download_batch_size: 1000,
                certificate_upload_batch_size: 1000,
                sender_certificate_download_batch_size: 1000,
                max_concurrent_batch_downloads: 10,
                max_joined_tasks: 100,
                allow_fast_blocks: false,
                notification_circuit_breaker_initial_probe_interval: Duration::from_secs(300),
                notification_circuit_breaker_max_probe_interval: Duration::from_secs(3600),
                max_event_stream_queries: 100,
            };
            let client = linera_core::client::Client::new(
                environment,
                wallet.genesis_config().admin_chain_id(),
                false,
                vec![],
                "lite-benchmark-full",
                None,
                None,
                10,
                chain_client_options,
                1000,
                1000,
                &Default::default(),
            );
            Some(Arc::new(client))
        } else {
            None
        };

    let mut chain_clients = Vec::new();
    for chain_id in chain_ids {
        let owner = wallet
            .get(chain_id)
            .and_then(|chain| chain.owner)
            .with_context(|| format!("chain {chain_id} has no owner in the wallet"))?;
        anyhow::ensure!(
            signer.contains_key(&owner).await.unwrap_or(false),
            "the keystore has no key for owner {owner} of chain {chain_id}"
        );
        let client = match &full_client {
            Some(full_client) => {
                let chain_client = full_client.create_chain_client(
                    chain_id,
                    None,
                    linera_base::data_types::BlockHeight(0),
                    &None,
                    Some(owner),
                    None,
                    false,
                );
                chain_client
                    .prepare_chain()
                    .await
                    .with_context(|| format!("failed to prepare chain {chain_id}"))?;
                AnyChainClient::Full(chain_client)
            }
            None => AnyChainClient::Lite(
                LiteChainClient::seed(
                    chain_id,
                    owner,
                    nodes.clone(),
                    committee.clone(),
                    signer.clone(),
                    args.light_certificates,
                )
                .await
                .with_context(|| {
                    format!("failed to seed the initial state for chain {chain_id}")
                })?,
            ),
        };
        chain_clients.push((client, chain_id, owner));
    }

    let shutdown = CancellationToken::new();
    if let Some(runtime_in_seconds) = args.runtime_in_seconds {
        let shutdown = shutdown.clone();
        task::spawn(async move {
            time::sleep(Duration::from_secs(runtime_in_seconds)).await;
            shutdown.cancel();
        });
    }

    let block_sizes = match &args.transactions_per_block_file {
        None => None,
        Some(path) => {
            let contents = fs::read_to_string(path)
                .with_context(|| format!("failed to read {}", path.display()))?;
            let sizes = parse_block_sizes(&contents, args.transactions_per_block_scale)
                .with_context(|| format!("failed to parse {}", path.display()))?;
            let total: usize = sizes.iter().sum();
            info!(
                entries = sizes.len(),
                min = sizes.iter().min(),
                max = sizes.iter().max(),
                mean = total as f64 / sizes.len() as f64,
                "using a block-size sequence from {}",
                path.display(),
            );
            Some(Arc::new(sizes))
        }
    };

    let num_chains = chain_clients.len();
    let success_count = Arc::new(AtomicUsize::new(0));
    let failure_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    // The current total bps (summed across all chains), read by every run_chain task each
    // tick and divided evenly by num_chains. Fixed for the whole run unless --bps-schedule is
    // given, in which case a background task updates it as the schedule progresses.
    let current_bps = Arc::new(AtomicUsize::new(args.bps));
    if let Some(spec) = &args.bps_schedule {
        let schedule = parse_bps_schedule(spec)?;
        info!(?schedule, "using a time-varying bps schedule");
        let current_bps = current_bps.clone();
        let shutdown = shutdown.clone();
        task::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(200));
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = interval.tick() => {
                        let elapsed = start.elapsed().as_secs_f64();
                        let bps = schedule
                            .iter()
                            .rev()
                            .find(|(offset, _)| (*offset as f64) <= elapsed)
                            .map_or(0, |(_, bps)| *bps);
                        current_bps.store(bps, Ordering::Relaxed);
                    }
                }
            }
        });
    }

    let mut join_set = task::JoinSet::new();
    for (client, chain_id, owner) in chain_clients {
        let current_bps = current_bps.clone();
        let shutdown = shutdown.clone();
        let success_count = success_count.clone();
        let failure_count = failure_count.clone();
        // Build the destination list and self-avoidance for the requested traffic mode. See
        // `TrafficMode`'s doc comments for the semantics of each variant.
        let (destinations, avoid_self) = match args.traffic_mode {
            TrafficMode::Independent => (vec![], true),
            TrafficMode::Full => (
                all_chain_ids
                    .iter()
                    .copied()
                    .filter(|id| *id != chain_id)
                    .collect(),
                true,
            ),
            TrafficMode::Mixed => (
                all_chain_ids
                    .iter()
                    .copied()
                    .filter(|id| *id != chain_id)
                    .flat_map(|other| [other, chain_id])
                    .collect(),
                false,
            ),
        };
        let generator = NativeFungibleTransferGenerator::new(
            chain_id,
            destinations,
            args.single_destination_per_block,
            avoid_self,
        )
        .map_err(|error| anyhow!("failed to create the operation generator: {error}"))?;
        // Either the same size for every block, or this chain's own random starting offset
        // into the shared block-size sequence, so that chains proposing at the same instant
        // don't all pick the same entry.
        let block_sizer = match &block_sizes {
            None => BlockSizer::Fixed(args.transactions_per_block),
            Some(sizes) => BlockSizer::Sequence {
                index: rand::thread_rng().gen_range(0..sizes.len()),
                sizes: sizes.clone(),
            },
        };
        // Spread each chain's first tick uniformly at random over one period of its own share
        // of the target rate, so e.g. 100 chains at a combined 50 bps don't all wake up and
        // propose a block in the same instant every 2 seconds. Based on the rate at spawn time
        // only: it's just meant to break up the initial thundering herd, not to track
        // --bps-schedule changes, and it disappears into the ordinary jitter of consensus
        // round-trips within the first few ticks anyway.
        let per_chain_bps = current_bps.load(Ordering::Relaxed) as f64 / num_chains.max(1) as f64;
        let de_sync_sleep = (per_chain_bps > 0.0)
            .then(|| Duration::from_secs_f64(rand::random::<f64>() / per_chain_bps));
        // Only `independent` mode never produces cross-chain messages; in every other mode each
        // block drains this chain's inboxes so they don't grow without bound (see
        // --max-incoming-bundles-per-block) -- unless --skip-message-processing asks to isolate
        // the sending side only.
        let process_messages =
            !matches!(args.traffic_mode, TrafficMode::Independent) && !args.skip_message_processing;
        join_set.spawn(run_chain(
            client,
            chain_id,
            generator,
            owner,
            current_bps,
            num_chains,
            block_sizer,
            process_messages,
            args.max_incoming_bundles_per_block,
            shutdown,
            success_count,
            failure_count,
            start,
            de_sync_sleep,
        ));
    }

    let report_success_count = success_count.clone();
    let report_failure_count = failure_count.clone();
    let report_shutdown = shutdown.clone();
    let report_task = task::spawn(async move {
        let mut interval = time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = report_shutdown.cancelled() => break,
                _ = interval.tick() => {
                    let successes = report_success_count.swap(0, Ordering::Relaxed);
                    let failures = report_failure_count.swap(0, Ordering::Relaxed);
                    info!(
                        "{} blocks/s committed, {} failed",
                        successes.to_formatted_string(&Locale::en),
                        failures.to_formatted_string(&Locale::en),
                    );
                }
            }
        }
    });

    let mut records = Vec::new();
    while let Some(result) = join_set.join_next().await {
        records.extend(result??);
    }
    shutdown.cancel();
    report_task.await?;

    if let Some(output_csv) = &args.output_csv {
        write_records_csv(output_csv, &records)
            .with_context(|| format!("failed to write {}", output_csv.display()))?;
    }

    Ok(())
}

/// Parses a `--bps-schedule` spec ("offset_seconds:bps,offset_seconds:bps,...") into a
/// sorted list of breakpoints. Must be non-empty and start at offset 0.
fn parse_bps_schedule(spec: &str) -> Result<Vec<(u64, usize)>> {
    let mut schedule = Vec::new();
    for part in spec.split(',') {
        let (offset_str, bps_str) = part
            .split_once(':')
            .ok_or_else(|| anyhow!("invalid --bps-schedule entry {part:?}, expected offset:bps"))?;
        let offset: u64 = offset_str
            .parse()
            .with_context(|| format!("invalid offset in --bps-schedule entry {part:?}"))?;
        let bps: usize = bps_str
            .parse()
            .with_context(|| format!("invalid bps in --bps-schedule entry {part:?}"))?;
        schedule.push((offset, bps));
    }
    schedule.sort_by_key(|(offset, _)| *offset);
    anyhow::ensure!(!schedule.is_empty(), "--bps-schedule must not be empty");
    anyhow::ensure!(
        schedule[0].0 == 0,
        "--bps-schedule's first offset must be 0"
    );
    Ok(schedule)
}

/// Parses the contents of a `--transactions-per-block-file`: one block size per line, with
/// blank lines and `#` comments ignored. Every size is multiplied by `scale`, rounded to the
/// nearest integer, and clamped to at least 1, since validators reject empty blocks.
fn parse_block_sizes(contents: &str, scale: f64) -> Result<Vec<usize>> {
    anyhow::ensure!(
        scale.is_finite() && scale > 0.0,
        "--transactions-per-block-scale must be a positive number, got {scale}"
    );
    let mut sizes = Vec::new();
    for (index, line) in contents.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let size: usize = line
            .parse()
            .with_context(|| format!("invalid block size {line:?} on line {}", index + 1))?;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the product is non-negative, and clamped below to a block size that is \
                      already far beyond anything a validator will accept"
        )]
        let scaled = (size as f64 * scale).round().clamp(1.0, u32::MAX as f64) as usize;
        sizes.push(scaled);
    }
    anyhow::ensure!(!sizes.is_empty(), "the block size file has no entries");
    Ok(sizes)
}

/// Writes one row per committed/failed block, matching `linera-paper-eval`'s `transfers.csv`
/// schema (a subset of its columns -- this tool has no notion of experiment/network_config/
/// repetition/phase, so those are omitted).
fn write_records_csv(path: &std::path::Path, records: &[BlockRecord]) -> Result<()> {
    let mut file = File::create(path)?;
    writeln!(
        file,
        "chain_id,ts_within_experiment,num_tx,num_bundles,result,duration_micros"
    )?;
    for record in records {
        writeln!(
            file,
            "{},{},{},{},{},{}",
            record.chain_id,
            record.elapsed_since_start.as_secs_f64(),
            record.num_tx,
            record.num_bundles,
            record.result,
            record.duration.as_micros(),
        )?;
    }
    Ok(())
}

/// Decides how many transactions the next block gets: either always the same number, or the
/// next entry of a shared sequence (see `--transactions-per-block-file`), cycled forever from
/// this chain's own starting offset.
enum BlockSizer {
    Fixed(usize),
    Sequence {
        sizes: Arc<Vec<usize>>,
        index: usize,
    },
}

impl BlockSizer {
    fn next_size(&mut self) -> usize {
        match self {
            BlockSizer::Fixed(size) => *size,
            BlockSizer::Sequence { sizes, index } => {
                let size = sizes[*index];
                *index = (*index + 1) % sizes.len();
                size
            }
        }
    }
}

/// Either client type `run_chain` can drive, dispatching each block to whichever one it
/// holds. See the module doc comment and `ClientMode` for the difference between them.
enum AnyChainClient {
    Lite(LiteChainClient),
    Full(FullChainClient),
}

impl AnyChainClient {
    /// Proposes and commits one block. `process_messages`/`bundle_cap` only apply to the lite
    /// client; the full client relies on `ChainClient`'s own inbox handling, and is currently
    /// only used with `--traffic-mode independent`, which never has bundles to drain (see
    /// `ClientMode::Full`'s doc comment).
    async fn propose_and_commit(
        &mut self,
        operations: Vec<Operation>,
        process_messages: bool,
        bundle_cap: usize,
    ) -> Result<usize> {
        match self {
            AnyChainClient::Lite(client) => {
                client
                    .propose_and_commit(operations, process_messages, bundle_cap)
                    .await
            }
            AnyChainClient::Full(client) => {
                match client.execute_operations(operations, vec![]).await {
                    Ok(ClientOutcome::Committed(_)) => Ok(0),
                    Ok(ClientOutcome::WaitForTimeout(_)) => {
                        Err(anyhow!("block did not commit: waiting for a round timeout"))
                    }
                    Ok(ClientOutcome::Conflict(_)) => Err(anyhow!(
                        "block did not commit: another block was committed first"
                    )),
                    Err(error) => Err(anyhow!("{error}")),
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_chain(
    mut client: AnyChainClient,
    chain_id: ChainId,
    mut generator: NativeFungibleTransferGenerator,
    owner: AccountOwner,
    current_bps: Arc<AtomicUsize>,
    num_chains: usize,
    mut block_sizer: BlockSizer,
    process_messages: bool,
    max_incoming_bundles_per_block: Option<usize>,
    shutdown: CancellationToken,
    success_count: Arc<AtomicUsize>,
    failure_count: Arc<AtomicUsize>,
    start: Instant,
    de_sync_sleep: Option<Duration>,
) -> Result<Vec<BlockRecord>> {
    // De-sync from every other chain's task before the first tick, so they don't all propose
    // a block in lockstep (see the comment where this is computed, in `main`). This delays
    // this chain's own first block by up to one of its periods, which slightly lowers the bps
    // this chain contributes near the very start of the run; negligible over any realistic
    // --runtime-in-seconds and outweighed by avoiding synchronized bursts against validators.
    if let Some(de_sync_sleep) = de_sync_sleep {
        time::sleep(de_sync_sleep).await;
    }

    let mut records = Vec::new();
    // The target rate can change over time (--bps-schedule), so the ticker is rebuilt
    // whenever it does; `tokio::time::interval` is kept (rather than a plain per-iteration
    // sleep) so a slow propose_and_commit still catches up on the next tick instead of
    // silently under-achieving the target rate. Divides in f64, not usize: with many chains
    // and a low total rate (e.g. 300 total / 31 clients / 10 chains), integer division could
    // truncate an intended-nonzero rate to exactly 0, silently stopping this chain's traffic
    // forever (until the schedule moves to a large-enough rate again) instead of just ticking
    // slowly.
    let mut ticking_at = 0.0_f64;
    let mut interval: Option<time::Interval> = None;
    loop {
        if shutdown.is_cancelled() {
            break;
        }
        let bps = current_bps.load(Ordering::Relaxed) as f64 / num_chains.max(1) as f64;
        if bps != ticking_at {
            ticking_at = bps;
            interval = if bps > 0.0 {
                let mut new_interval = time::interval(Duration::from_secs_f64(1.0 / bps));
                // The default (`Burst`) fires every missed tick back-to-back with no pacing
                // gap once behind schedule, trying to catch up to the *original* schedule --
                // fine when the target period has huge headroom over real block latency (as
                // in every run before --simulated-latency-ms existed), but once real per-block
                // latency approaches the target period, a single transient slowdown makes a
                // chain burst-fire at far above its target rate, which slows other chains down
                // too, which burst-fires them -- a thundering-herd feedback loop. `Delay` paces
                // forward from whenever the last tick actually completed instead.
                new_interval.set_missed_tick_behavior(time::MissedTickBehavior::Delay);
                Some(new_interval)
            } else {
                None
            };
        }
        match &mut interval {
            Some(interval) => interval.tick().await,
            None => {
                time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let operations = generator.generate_operations(owner, block_sizer.next_size());
        let num_tx = operations.len();
        // Cap the incoming bundles this block will drain: an explicit limit, or twice the
        // block's own operation count, so a backlog is spread over several blocks instead of
        // one huge one, and the cap tracks the (possibly variable) block size. Unused when
        // `process_messages` is false.
        let bundle_cap = max_incoming_bundles_per_block.unwrap_or_else(|| num_tx.saturating_mul(2));
        let before = Instant::now();
        let outcome = client
            .propose_and_commit(operations, process_messages, bundle_cap)
            .await;
        let duration = before.elapsed();
        let (result, num_bundles) = match outcome {
            Ok(num_bundles) => {
                success_count.fetch_add(1, Ordering::Relaxed);
                ("committed", num_bundles)
            }
            Err(error) => {
                warn!(%chain_id, %error, "failed to commit a block");
                failure_count.fetch_add(1, Ordering::Relaxed);
                ("failed", 0)
            }
        };
        records.push(BlockRecord {
            chain_id,
            elapsed_since_start: before.saturating_duration_since(start),
            num_tx,
            num_bundles,
            result,
            duration,
        });
    }
    info!(%chain_id, "stopping benchmark");
    Ok(records)
}

/// One committed or failed block, timed relative to the benchmark's start. See
/// `write_records_csv` for the on-disk schema.
struct BlockRecord {
    chain_id: ChainId,
    elapsed_since_start: Duration,
    num_tx: usize,
    num_bundles: usize,
    result: &'static str,
    duration: Duration,
}

/// Tracks just enough state about one chain to keep proposing valid blocks, without any
/// local storage or execution.
struct LiteChainClient {
    chain_id: ChainId,
    owner: AccountOwner,
    epoch: Epoch,
    height: linera_base::data_types::BlockHeight,
    previous_block_hash: Option<CryptoHash>,
    nodes: Vec<(ValidatorPublicKey, Client)>,
    committee: Committee,
    signer: InMemorySigner,
    value_cache: ValueCache<CryptoHash, ConfirmedBlockCertificate>,
    /// The incoming message bundles to drain into the *next* block, computed as a side effect
    /// of the previous block's confirmed-value fetch (see `propose_and_commit`). Held across
    /// blocks so that draining costs no extra round trip; empty before the first block and in
    /// `independent` mode.
    pending_bundles: Vec<IncomingBundle>,
    /// Whether to broadcast the confirmed certificate in its compact, value-free form where
    /// possible (see `--light-certificates`).
    light_certificates: bool,
}

impl LiteChainClient {
    /// Seeds the client's state for `chain_id` from the first validator that answers.
    async fn seed(
        chain_id: ChainId,
        owner: AccountOwner,
        nodes: Vec<(ValidatorPublicKey, Client)>,
        committee: Committee,
        signer: InMemorySigner,
        light_certificates: bool,
    ) -> Result<Self> {
        for (public_key, node) in &nodes {
            let query = ChainInfoQuery::new(chain_id);
            match node.handle_chain_info_query(query).await {
                Ok(response) => {
                    let info = response.info;
                    return Ok(Self {
                        chain_id,
                        owner,
                        epoch: info.epoch,
                        height: info.next_block_height,
                        previous_block_hash: info.block_hash,
                        nodes,
                        committee,
                        signer,
                        value_cache: ValueCache::new("lite-benchmark", 64, 60),
                        pending_bundles: Vec::new(),
                        light_certificates,
                    });
                }
                Err(error) => {
                    warn!(%public_key, %error, "validator did not answer the initial chain info query");
                }
            }
        }
        bail!("no validator answered the initial chain info query");
    }

    /// Builds, signs, and submits a block with the given operations, then drives it to a
    /// committed certificate. Uses `Round::Fast`, so this only works on chains owned by a
    /// single super owner.
    ///
    /// If `process_messages` is set, the block first drains up to `bundle_cap` incoming message
    /// bundles from this chain's inboxes (as `Transaction::ReceiveMessages`, before the
    /// operations), so the inboxes don't grow without bound in cross-chain traffic modes.
    /// Returns the number of bundles that were included.
    ///
    /// The bundles come from `self.pending_bundles`, which the *previous* block's confirmed-value
    /// fetch computed for us -- so draining costs no extra round trip. This is sound because a
    /// block only removes its bundles from the validators' inboxes once it commits: the bundles
    /// we carried over are still pending (nothing else drains this chain), and still present in
    /// the validators that reported them, so the proposal is accepted. `self.pending_bundles` is
    /// only refreshed after this block commits, so a failed block simply retries the same set.
    async fn propose_and_commit(
        &mut self,
        operations: Vec<linera_execution::Operation>,
        process_messages: bool,
        bundle_cap: usize,
    ) -> Result<usize> {
        let bundles: Vec<IncomingBundle> = if process_messages {
            self.pending_bundles
                .iter()
                .take(bundle_cap)
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        let num_bundles = bundles.len();
        // The bundles this block consumes, so the next block's pending set can exclude them (the
        // confirmed-value fetch below sees them still in the inboxes, since our certificate has
        // not been broadcast yet).
        let consumed: HashSet<_> = bundles
            .iter()
            .map(|bundle| (bundle.origin, bundle.bundle.cursor()))
            .collect();
        let transactions = bundles
            .into_iter()
            .map(Transaction::ReceiveMessages)
            .chain(operations.into_iter().map(Transaction::ExecuteOperation))
            .collect();
        let block = ProposedBlock {
            chain_id: self.chain_id,
            epoch: self.epoch,
            transactions,
            height: self.height,
            timestamp: Timestamp::now(),
            authenticated_owner: Some(self.owner),
            previous_block_hash: self.previous_block_hash,
        };
        let proposal = BlockProposal::new_initial(self.owner, Round::Fast, block, &self.signer)
            .await
            .map_err(|error| anyhow!("failed to sign the block proposal: {error}"))?;

        // Broadcast the proposal to every validator and collect their `ConfirmedBlock` votes.
        let responses = join_all(self.nodes.iter().map(|(public_key, node)| {
            let proposal = proposal.clone();
            let public_key = *public_key;
            let node = node.clone();
            async move { (public_key, node.handle_block_proposal(proposal).await) }
        }))
        .await;
        let votes = responses
            .into_iter()
            .filter_map(|(public_key, result)| match result {
                Ok(response) => response.info.manager.pending.map(|vote| (public_key, vote)),
                Err(error) => {
                    warn!(%public_key, %error, "validator rejected the block proposal");
                    None
                }
            });
        let (value_hash, signatures) =
            find_confirming_quorum(self.chain_id, votes, &self.committee)
                .context("no quorum of validators voted to confirm the proposed block")?;

        // Fetch the confirmed value (with its real execution outcome) instead of executing the
        // block ourselves, and -- folded into the same round trip -- the inboxes' pending
        // bundles, from which we compute the set to drain into the *next* block.
        let (confirmed_block, next_pending) = self
            .fetch_confirmed_and_pending(value_hash, process_messages, &consumed)
            .await?;

        // The vote's `first_round` attestation must be reproduced exactly, since it is part of
        // what every signature covers (see `Vote::new_with_first_round`); a single super owner's
        // `Round::Fast` is always the chain's designated first round, so this is always `true`.
        let quorum = GenericCertificate::new_with_payload(
            confirmed_block,
            Round::Fast,
            None,
            true,
            None,
            signatures,
        );
        let certificate =
            ConfirmedBlockCertificate::from_parts(quorum, JustificationChain::default());
        let cached_certificate = self
            .value_cache
            .insert(&certificate.hash(), certificate.clone());

        // Broadcast the certificate so every validator commits the block. Only advance our own
        // state once at least one validator actually accepted it, so we don't get out of sync
        // with the chain if the certificate is rejected everywhere.
        //
        // With --light-certificates, prefer sending each validator just the certificate's hash
        // and signatures (no block value) via RemoteNode::handle_optimized_confirmed_certificate
        // -- every validator here voted on this block in the first round trip, so it already has
        // the value cached and can reconstruct the full certificate locally. A validator that
        // fell behind and forgot the value it signed gets a transparent fallback to the full
        // certificate (see that method's doc comment). This only shrinks this round trip's
        // payload; it doesn't remove it.
        let light_certificates = self.light_certificates;
        let results = join_all(self.nodes.iter().map(|(public_key, node)| {
            let node = node.clone();
            let cached_certificate = cached_certificate.clone();
            async move {
                if light_certificates {
                    let remote_node = RemoteNode {
                        public_key: *public_key,
                        node,
                    };
                    remote_node
                        .handle_optimized_confirmed_certificate(
                            &cached_certificate,
                            CrossChainMessageDelivery::NonBlocking,
                        )
                        .await
                        .map(|_| ())
                } else {
                    node.handle_confirmed_certificate(
                        cached_certificate,
                        CrossChainMessageDelivery::NonBlocking,
                    )
                    .await
                    .map(|_| ())
                }
            }
        }))
        .await;
        let mut committed = false;
        for result in results {
            if let Err(error) = result {
                warn!(%error, "validator failed to process the confirmed certificate");
            } else {
                committed = true;
            }
        }
        anyhow::ensure!(committed, "no validator accepted the confirmed certificate");

        self.previous_block_hash = Some(certificate.hash());
        self.height = self.height.try_add_one()?;
        // Only now that the block committed (so its bundles are being removed from the inboxes)
        // do we adopt the next pending set. On a failed block we keep `self.pending_bundles` as
        // it was, so the next attempt retries the same, still-pending bundles.
        self.pending_bundles = next_pending;
        Ok(num_bundles)
    }

    /// In one parallel round trip to every validator, fetches the confirmed block value for
    /// `value_hash` (from any validator that has it) and, if `process_messages` is set, the
    /// bundles to drain into the *next* block.
    ///
    /// The next pending set is the per-origin prefix that *every* responding validator agrees
    /// on, minus `consumed` (the bundles this block is about to remove, which are still in the
    /// inboxes at query time since our certificate has not been broadcast yet). We take only the
    /// agreed prefix because certificates are delivered non-blocking, so the validators' inboxes
    /// are not in lockstep: a bundle one validator already holds may not have reached another. A
    /// proposal is rejected wholesale if it receives a bundle a validator lacks
    /// (`MissingCrossChainUpdate`), and a given origin's bundles must be consumed in cursor order
    /// (`IncorrectOrder`), so anything not yet everywhere is simply left for a later block. No
    /// validator response is trusted for anything but which bundles exist; they are copied
    /// verbatim into the block. The result is not capped here -- the cap is applied when the
    /// bundles are actually included, so a backlog beyond one block's cap carries forward.
    async fn fetch_confirmed_and_pending(
        &self,
        value_hash: CryptoHash,
        process_messages: bool,
        consumed: &HashSet<(ChainId, linera_base::data_types::Cursor)>,
    ) -> Result<(ConfirmedBlock, Vec<IncomingBundle>)> {
        let responses = join_all(self.nodes.iter().map(|(public_key, node)| {
            let node = node.clone();
            let mut query = ChainInfoQuery::new(self.chain_id);
            query.request_manager_values = true;
            if process_messages {
                query = query.with_pending_message_bundles();
            }
            let public_key = *public_key;
            async move {
                match node.handle_chain_info_query(query).await {
                    Ok(response) => Some(response.info),
                    Err(error) => {
                        warn!(%public_key, %error, "validator did not answer the confirmed-value query");
                        None
                    }
                }
            }
        }))
        .await;

        let mut confirmed_block: Option<ConfirmedBlock> = None;
        let mut per_node: Vec<Vec<IncomingBundle>> = Vec::new();
        for info in responses.into_iter().flatten() {
            if process_messages {
                per_node.push(info.requested_pending_message_bundles);
            }
            if confirmed_block.is_none() {
                if let Some(value) = info.manager.requested_confirmed {
                    if value.hash() == value_hash {
                        confirmed_block = Some(*value);
                    }
                }
            }
        }
        let confirmed_block =
            confirmed_block.context("could not fetch the confirmed block value")?;

        let next_pending = if process_messages {
            common_prefix_bundles(per_node)
                .into_iter()
                .filter(|bundle| !consumed.contains(&(bundle.origin, bundle.bundle.cursor())))
                .collect()
        } else {
            Vec::new()
        };
        Ok((confirmed_block, next_pending))
    }
}

/// Given each responding validator's list of pending incoming bundles, returns the bundles that
/// appear -- as an in-order per-origin prefix -- in *every* list. Bundles from one origin are
/// FIFO by cursor, so for each origin this compares the lists element by element and keeps the
/// longest common leading run; an origin missing from any list contributes nothing. Origins are
/// visited in a deterministic (sorted) order. See `fetch_incoming_bundles` for why only this
/// safe intersection is used.
fn common_prefix_bundles(per_node: Vec<Vec<IncomingBundle>>) -> Vec<IncomingBundle> {
    let Some((first, rest)) = per_node.split_first() else {
        return Vec::new();
    };
    // Group each node's bundles by origin, preserving each origin's cursor order.
    let group = |bundles: &[IncomingBundle]| -> BTreeMap<ChainId, Vec<IncomingBundle>> {
        let mut by_origin: BTreeMap<ChainId, Vec<IncomingBundle>> = BTreeMap::new();
        for bundle in bundles {
            by_origin
                .entry(bundle.origin)
                .or_default()
                .push(bundle.clone());
        }
        by_origin
    };
    let base = group(first);
    let others: Vec<_> = rest.iter().map(|node| group(node)).collect();
    let mut result = Vec::new();
    for (origin, base_bundles) in base {
        let mut prefix_len = base_bundles.len();
        for other in &others {
            let other_bundles = other.get(&origin).map_or(&[][..], Vec::as_slice);
            let matching = base_bundles
                .iter()
                .zip(other_bundles)
                .take_while(|(a, b)| a.bundle.cursor() == b.bundle.cursor())
                .count();
            prefix_len = prefix_len.min(matching);
            if prefix_len == 0 {
                break;
            }
        }
        result.extend(base_bundles.into_iter().take(prefix_len));
    }
    result
}

/// Groups the given validator votes by the `ConfirmedBlock` value hash they attest to, and
/// returns the first hash (and its signatures) whose combined committee weight reaches the
/// quorum threshold. Votes for the wrong chain or of the wrong kind are ignored. No signature
/// is verified here: the caller trusts every vote at face value.
fn find_confirming_quorum(
    chain_id: ChainId,
    votes: impl IntoIterator<Item = (ValidatorPublicKey, linera_chain::data_types::LiteVote)>,
    committee: &Committee,
) -> Option<(CryptoHash, Vec<(ValidatorPublicKey, ValidatorSignature)>)> {
    let mut signatures_by_hash: HashMap<CryptoHash, Vec<(ValidatorPublicKey, ValidatorSignature)>> =
        HashMap::new();
    let mut weight_by_hash: HashMap<CryptoHash, u64> = HashMap::new();
    for (public_key, vote) in votes {
        if vote.value.chain_id != chain_id || vote.value.kind != CertificateKind::Confirmed {
            continue;
        }
        let hash = vote.value.value_hash;
        signatures_by_hash
            .entry(hash)
            .or_default()
            .push((public_key, vote.signature));
        let weight = weight_by_hash.entry(hash).or_insert(0);
        *weight += committee.weight(&public_key);
        if *weight >= committee.quorum_threshold() {
            let signatures = signatures_by_hash
                .remove(&hash)
                .expect("just inserted above");
            return Some((hash, signatures));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use linera_base::{
        crypto::{AccountSecretKey, CryptoHash, ValidatorKeypair},
        data_types::BlockHeight,
    };
    use linera_chain::data_types::{LiteValue, LiteVote, MessageAction, MessageBundle};

    use super::*;

    /// A pending bundle from `origin` whose cursor is `(height, index)`. The message list is
    /// empty: `common_prefix_bundles` compares only cursors, so the contents are irrelevant.
    fn bundle(origin: ChainId, height: u64, index: u32) -> IncomingBundle {
        IncomingBundle {
            origin,
            bundle: MessageBundle {
                height: BlockHeight(height),
                timestamp: Timestamp::from(0),
                certificate_hash: CryptoHash::test_hash("cert"),
                transaction_index: index,
                messages: Vec::new(),
            },
            action: MessageAction::Accept,
        }
    }

    /// The bundles' cursors, sorted by (origin, height, index). Sorting makes comparisons
    /// insensitive to the order origins are emitted in (which is irrelevant, since each origin's
    /// inbox is drained independently) while still exposing any per-origin reordering, because
    /// within an origin the expected cursors are already ascending.
    fn cursors(bundles: &[IncomingBundle]) -> Vec<(ChainId, u64, u32)> {
        let mut cursors: Vec<_> = bundles
            .iter()
            .map(|b| (b.origin, b.bundle.height.0, b.bundle.transaction_index))
            .collect();
        cursors.sort();
        cursors
    }

    fn sorted(mut cursors: Vec<(ChainId, u64, u32)>) -> Vec<(ChainId, u64, u32)> {
        cursors.sort();
        cursors
    }

    #[test]
    fn common_prefix_takes_the_agreed_per_origin_prefix() {
        let a = ChainId(CryptoHash::test_hash("a"));
        let b = ChainId(CryptoHash::test_hash("b"));

        // No responders at all -> nothing to drain.
        assert!(common_prefix_bundles(Vec::new()).is_empty());

        // A single responder: everything it lists is included (grouped by origin, in order).
        let only = vec![bundle(a, 0, 0), bundle(a, 1, 0), bundle(b, 0, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![only.clone()])),
            sorted(vec![(a, 0, 0), (a, 1, 0), (b, 0, 0)]),
        );

        // Two responders agreeing fully: the whole thing survives.
        assert_eq!(
            common_prefix_bundles(vec![only.clone(), only.clone()]).len(),
            3
        );

        // One responder is one bundle behind on origin `a`: only the shared prefix of `a`
        // survives, and origin `b`, present in both, is kept.
        let ahead = vec![bundle(a, 0, 0), bundle(a, 1, 0), bundle(b, 0, 0)];
        let behind = vec![bundle(a, 0, 0), bundle(b, 0, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![ahead, behind])),
            sorted(vec![(a, 0, 0), (b, 0, 0)]),
        );

        // The lists diverge mid-origin (a different cursor at index 1): the prefix stops at the
        // divergence, and nothing past it is included even though later cursors happen to match.
        let left = vec![bundle(a, 0, 0), bundle(a, 1, 0), bundle(a, 2, 0)];
        let right = vec![bundle(a, 0, 0), bundle(a, 5, 0), bundle(a, 2, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![left, right])),
            vec![(a, 0, 0)],
        );

        // An origin missing from one responder contributes nothing, but other shared origins
        // are unaffected.
        let with_b = vec![bundle(a, 0, 0), bundle(b, 0, 0)];
        let without_b = vec![bundle(a, 0, 0)];
        assert_eq!(
            cursors(&common_prefix_bundles(vec![with_b, without_b])),
            vec![(a, 0, 0)],
        );
    }

    fn committee_of(size: usize) -> (Committee, Vec<ValidatorPublicKey>) {
        let keys: Vec<_> = (0..size)
            .map(|_| {
                (
                    ValidatorKeypair::generate().public_key,
                    AccountSecretKey::generate().public(),
                )
            })
            .collect();
        let public_keys = keys.iter().map(|(key, _)| *key).collect();
        (Committee::make_simple(keys), public_keys)
    }

    fn vote(chain_id: ChainId, value_hash: CryptoHash) -> LiteVote {
        LiteVote {
            value: LiteValue {
                value_hash,
                chain_id,
                kind: CertificateKind::Confirmed,
            },
            round: Round::Fast,
            unlocking_round: None,
            first_round: true,
            justification_commitment: None,
            signature: ValidatorSignature::sign_prehash(
                &ValidatorKeypair::generate().secret_key,
                value_hash,
            ),
        }
    }

    #[test]
    fn block_sizes_are_scaled_and_never_zero() {
        let contents = "# a comment\n100\n\n  3 \n0\n";
        assert_eq!(parse_block_sizes(contents, 1.0).unwrap(), vec![100, 3, 1]);
        // 3 * 0.5 rounds to 2, and 0 is clamped up to 1: validators reject empty blocks.
        assert_eq!(parse_block_sizes(contents, 0.5).unwrap(), vec![50, 2, 1]);
        assert_eq!(parse_block_sizes(contents, 2.0).unwrap(), vec![200, 6, 1]);

        assert!(parse_block_sizes("12\nnot a number\n", 1.0).is_err());
        assert!(parse_block_sizes("# nothing but comments\n", 1.0).is_err());
        assert!(parse_block_sizes("12\n", 0.0).is_err());
    }

    #[test]
    fn a_block_size_sequence_cycles_from_its_offset() {
        let mut sizer = BlockSizer::Sequence {
            sizes: Arc::new(vec![1, 2, 3]),
            index: 2,
        };
        let sizes: Vec<_> = (0..5).map(|_| sizer.next_size()).collect();
        assert_eq!(sizes, vec![3, 1, 2, 3, 1]);

        let mut sizer = BlockSizer::Fixed(7);
        assert_eq!(sizer.next_size(), 7);
        assert_eq!(sizer.next_size(), 7);
    }

    #[test]
    fn quorum_is_reached_once_enough_weight_agrees() {
        let chain_id = ChainId(CryptoHash::test_hash("chain"));
        let value_hash = CryptoHash::test_hash("confirmed-block");
        let (committee, keys) = committee_of(4);

        // Only 2 out of 4 equally-weighted validators agree: not a quorum yet.
        let votes = keys[..2]
            .iter()
            .map(|key| (*key, vote(chain_id, value_hash)));
        assert!(find_confirming_quorum(chain_id, votes, &committee).is_none());

        // 3 out of 4 is enough.
        let votes = keys[..3]
            .iter()
            .map(|key| (*key, vote(chain_id, value_hash)));
        let (hash, signatures) = find_confirming_quorum(chain_id, votes, &committee)
            .expect("3 out of 4 equally-weighted validators should reach the quorum threshold");
        assert_eq!(hash, value_hash);
        assert_eq!(signatures.len(), 3);
    }

    #[test]
    fn votes_for_a_different_chain_are_ignored() {
        let chain_id = ChainId(CryptoHash::test_hash("chain"));
        let other_chain_id = ChainId(CryptoHash::test_hash("other-chain"));
        let value_hash = CryptoHash::test_hash("confirmed-block");
        let (committee, keys) = committee_of(4);

        let votes = keys
            .iter()
            .map(|key| (*key, vote(other_chain_id, value_hash)));
        assert!(find_confirming_quorum(chain_id, votes, &committee).is_none());
    }

    #[test]
    fn a_split_vote_never_reaches_quorum_on_either_side() {
        let chain_id = ChainId(CryptoHash::test_hash("chain"));
        let hash_a = CryptoHash::test_hash("block-a");
        let hash_b = CryptoHash::test_hash("block-b");
        let (committee, keys) = committee_of(4);

        let votes = vec![
            (keys[0], vote(chain_id, hash_a)),
            (keys[1], vote(chain_id, hash_a)),
            (keys[2], vote(chain_id, hash_b)),
            (keys[3], vote(chain_id, hash_b)),
        ];
        assert!(find_confirming_quorum(chain_id, votes, &committee).is_none());
    }
}
