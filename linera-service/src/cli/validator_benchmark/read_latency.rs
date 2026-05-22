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
    report::{
        PerChainReadBaseline, PerChainReadStress, ReadBaselineReport, ReadStressReport, StressLevel,
    },
};

/// L2: send `requests_per_chain` sequential chain-info queries per chain.
pub async fn run_baseline<N: ValidatorNode>(
    node: &N,
    chains: &[ChainId],
    requests_per_chain: usize,
) -> ReadBaselineReport {
    let mut per_chain = Vec::with_capacity(chains.len());
    for chain in chains {
        let mut samples = Samples::new();
        for _ in 0..requests_per_chain {
            let query = ChainInfoQuery::new(*chain);
            let start = Instant::now();
            match node.handle_chain_info_query(query).await {
                Ok(_) => samples.record_success(start.elapsed().as_secs_f64() * 1000.0),
                Err(e) => samples.record_error(categorize(&e.to_string())),
            }
        }
        per_chain.push(PerChainReadBaseline {
            chain_id: chain.to_string(),
            latency_ms: samples.summary(),
        });
    }
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
) -> ReadStressReport
where
    N: ValidatorNode + Clone + Send + Sync + 'static,
{
    let mut per_chain = Vec::with_capacity(chains.len());
    for chain in chains {
        let mut level_reports = Vec::with_capacity(levels.len());
        for &concurrency in levels {
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
        }
        per_chain.push(PerChainReadStress {
            chain_id: chain.to_string(),
            levels: level_reports,
        });
    }
    ReadStressReport { per_chain }
}
