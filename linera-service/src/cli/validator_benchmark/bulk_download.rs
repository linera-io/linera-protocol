// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L4 bulk download: heavy read path via `download_certificates_by_heights`.
//!
//! Exercises a different I/O shape from L2/L3 (large batched payloads rather
//! than many tiny RPCs). The default `auto` height range targets the most
//! recent heights the candidate already has, so the only variable under test is
//! the read path, not history availability.

use std::time::Instant;

use anyhow::Result;
use linera_base::{data_types::BlockHeight, identifiers::ChainId};
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use tokio::task::JoinSet;

use super::{
    latency::Samples,
    preflight::categorize,
    progress::{Phase, Progress},
    report::{BulkDownloadReport, BulkRun, PerChainBulk},
};

/// How many batches the `auto` range spans.
const AUTO_BATCH_COUNT: u64 = 100;

pub async fn run<N>(
    node: &N,
    chains: &[ChainId],
    batch_size: u32,
    concurrencies: &[usize],
    height_range_arg: &str,
    progress: &Progress,
) -> Result<BulkDownloadReport>
where
    N: ValidatorNode + Clone + Send + Sync + 'static,
{
    let phase = progress.phase("L4 bulk download", Some(0));
    let mut per_chain = Vec::with_capacity(chains.len());
    for &chain in chains {
        let info = node.handle_chain_info_query(ChainInfoQuery::new(chain)).await?;
        let tip = info.info.next_block_height.0;
        let (from, to) = resolve_range(height_range_arg, tip, batch_size)?;
        let batches = (from..to).step_by(batch_size.max(1) as usize).count() as u64;
        phase.inc_length(batches * concurrencies.len() as u64);

        let mut runs = Vec::with_capacity(concurrencies.len());
        for &concurrency in concurrencies {
            runs.push(run_one(node, chain, batch_size, concurrency.max(1), from, to, &phase).await);
        }
        per_chain.push(PerChainBulk {
            chain_id: chain.to_string(),
            runs,
        });
    }
    phase.finish_ok();
    Ok(BulkDownloadReport { per_chain })
}

/// Run a single (chain, concurrency) batch download over `[from, to)`.
async fn run_one<N>(
    node: &N,
    chain: ChainId,
    batch_size: u32,
    concurrency: usize,
    from: u64,
    to: u64,
    phase: &Phase,
) -> BulkRun
where
    N: ValidatorNode + Clone + Send + Sync + 'static,
{
    let batch_starts: Vec<u64> = (from..to).step_by(batch_size.max(1) as usize).collect();
    let mut set: JoinSet<(Result<u64, String>, u64, f64)> = JoinSet::new();
    let mut samples = Samples::new();
    let mut bytes_in: u64 = 0;
    let mut certs_received: u64 = 0;

    let started = Instant::now();
    let mut next = 0;
    // Keep at most `concurrency` batch requests in flight at once.
    while next < batch_starts.len() || !set.is_empty() {
        while set.len() < concurrency && next < batch_starts.len() {
            let start_height = batch_starts[next];
            next += 1;
            let end_height = (start_height + batch_size as u64).min(to);
            let heights: Vec<BlockHeight> = (start_height..end_height).map(BlockHeight).collect();
            let node = node.clone();
            set.spawn(async move {
                let t0 = Instant::now();
                let result = node.download_certificates_by_heights(chain, heights).await;
                let elapsed = t0.elapsed().as_secs_f64() * 1000.0;
                match result {
                    Ok(certs) => {
                        let bytes: u64 = certs
                            .iter()
                            .map(|c| bcs::serialized_size(c).unwrap_or(0) as u64)
                            .sum();
                        (Ok(certs.len() as u64), bytes, elapsed)
                    }
                    Err(e) => (Err(categorize(&e.to_string()).to_string()), 0, elapsed),
                }
            });
        }
        if let Some(joined) = set.join_next().await {
            match joined {
                Ok((Ok(count), bytes, ms)) => {
                    samples.record_success(ms);
                    certs_received += count;
                    bytes_in += bytes;
                }
                Ok((Err(category), _, _)) => samples.record_error(category),
                Err(e) => samples.record_error(format!("join: {e}")),
            }
            phase.inc(1);
        }
    }

    let duration = started.elapsed().as_secs_f64().max(f64::EPSILON);
    BulkRun {
        concurrency,
        batch_size,
        heights_range: (from, to),
        bytes_in,
        certs_received,
        duration_secs: duration,
        mb_per_sec: (bytes_in as f64 / 1_048_576.0) / duration,
        certs_per_sec: certs_received as f64 / duration,
        latency_ms: samples.summary(),
    }
}

/// Resolve the height range: `auto` targets the most recent
/// `batch_size * AUTO_BATCH_COUNT` heights up to the tip; otherwise `FROM:TO`.
fn resolve_range(arg: &str, tip: u64, batch_size: u32) -> Result<(u64, u64)> {
    if arg == "auto" {
        let span = batch_size as u64 * AUTO_BATCH_COUNT;
        Ok((tip.saturating_sub(span), tip))
    } else {
        let (a, b) = arg
            .split_once(':')
            .ok_or_else(|| anyhow::anyhow!("--bulk-height-range must be `auto` or `FROM:TO`"))?;
        let from: u64 = a.trim().parse()?;
        let to: u64 = b.trim().parse()?;
        if to < from {
            anyhow::bail!("--bulk-height-range FROM must not exceed TO");
        }
        Ok((from, to))
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_range;

    #[test]
    fn auto_under_tip() {
        assert_eq!(resolve_range("auto", 50_000, 100).unwrap(), (40_000, 50_000));
    }

    #[test]
    fn auto_clamps_to_zero() {
        assert_eq!(resolve_range("auto", 50, 100).unwrap(), (0, 50));
    }

    #[test]
    fn explicit_range() {
        assert_eq!(resolve_range("100:200", 999, 10).unwrap(), (100, 200));
    }

    #[test]
    fn explicit_range_inverted_rejected() {
        assert!(resolve_range("200:100", 999, 10).is_err());
    }

    #[test]
    fn invalid_range_rejected() {
        assert!(resolve_range("bad", 999, 10).is_err());
    }
}
