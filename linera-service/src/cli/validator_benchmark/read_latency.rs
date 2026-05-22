// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L2 read latency baseline and L3 read stress (concurrency ramp).
//!
//! Both layers exercise the cheapest read RPC, `handle_chain_info_query`. L2
//! runs strictly sequentially to measure the queue-free latency floor; L3 ramps
//! concurrency to find the sustained-throughput and saturation behavior.

use std::time::{Duration, Instant};

use linera_base::identifiers::ChainId;
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use tokio::{task::JoinSet, time::Instant as TokioInstant};

use super::{
    latency::Samples,
    preflight::categorize,
    progress::Progress,
    report::{
        PerChainReadBaseline, PerChainReadStress, ReadBaselineReport, ReadStressReport, StressLevel,
    },
};

/// First 8 hex chars of a chain id, for compact progress messages.
fn short(chain: &ChainId) -> String {
    chain.to_string().chars().take(8).collect()
}

/// L2: send `requests_per_chain` sequential chain-info queries per chain.
pub async fn run_baseline<N: ValidatorNode>(
    node: &N,
    chains: &[ChainId],
    requests_per_chain: usize,
    progress: &Progress,
) -> ReadBaselineReport {
    let phase = progress.phase(
        "L2 read baseline",
        Some((chains.len() * requests_per_chain) as u64),
    );
    let mut per_chain = Vec::with_capacity(chains.len());
    for chain in chains {
        phase.set_message(format!("chain {}", short(chain)));
        let mut samples = Samples::new();
        for _ in 0..requests_per_chain {
            let query = ChainInfoQuery::new(*chain);
            let start = Instant::now();
            match node.handle_chain_info_query(query).await {
                Ok(_) => samples.record_success(start.elapsed().as_secs_f64() * 1000.0),
                Err(e) => samples.record_error(categorize(&e.to_string())),
            }
            phase.inc(1);
        }
        per_chain.push(PerChainReadBaseline {
            chain_id: chain.to_string(),
            latency_ms: samples.summary(),
        });
    }
    phase.finish_ok();
    ReadBaselineReport { per_chain }
}

/// L3: for each chain, ramp through `levels` concurrent workers, each level for
/// `duration`. Each worker owns its own [`Samples`] (no shared mutex, so lock
/// contention does not distort the latency measurement) and the parent merges.
pub async fn run_stress<N>(
    node: &N,
    chains: &[ChainId],
    levels: &[usize],
    duration: Duration,
    progress: &Progress,
) -> ReadStressReport
where
    N: ValidatorNode + Clone + Send + Sync + 'static,
{
    let phase = progress.phase("L3 read stress", Some((chains.len() * levels.len()) as u64));
    let mut per_chain = Vec::with_capacity(chains.len());
    for chain in chains {
        let mut level_reports = Vec::with_capacity(levels.len());
        for &concurrency in levels {
            phase.set_message(format!("chain {} · conc {}", short(chain), concurrency));
            let deadline = TokioInstant::now() + duration;
            let mut set: JoinSet<Samples> = JoinSet::new();
            for _ in 0..concurrency.max(1) {
                let node = node.clone();
                let chain = *chain;
                set.spawn(async move {
                    let mut samples = Samples::new();
                    while TokioInstant::now() < deadline {
                        let query = ChainInfoQuery::new(chain);
                        let start = Instant::now();
                        match node.handle_chain_info_query(query).await {
                            Ok(_) => samples.record_success(start.elapsed().as_secs_f64() * 1000.0),
                            Err(e) => samples.record_error(categorize(&e.to_string())),
                        }
                    }
                    samples
                });
            }

            let mut merged = Samples::new();
            while let Some(joined) = set.join_next().await {
                match joined {
                    Ok(samples) => merged.merge_from(samples),
                    Err(e) => merged.record_error(format!("join: {e}")),
                }
            }

            let summary = merged.summary();
            let completed = summary.count;
            level_reports.push(StressLevel {
                concurrency,
                duration_secs: duration.as_secs(),
                completed,
                throughput_per_sec: completed as f64 / duration.as_secs_f64().max(f64::EPSILON),
                latency_ms: summary,
            });
            phase.inc(1);
        }
        per_chain.push(PerChainReadStress {
            chain_id: chain.to_string(),
            levels: level_reports,
        });
    }
    phase.finish_ok();
    ReadStressReport { per_chain }
}
