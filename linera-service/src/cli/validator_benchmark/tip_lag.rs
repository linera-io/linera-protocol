// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L5 tip-lag snapshot.
//!
//! Compares the candidate's reported tip against a committee-backed reference
//! tip (obtained through the local client's normal synchronization path, not by
//! benchmarking the committee). Repeated sampling reveals whether the candidate
//! is keeping up, catching up, or falling behind.

use std::time::Duration;

use anyhow::Result;
use linera_base::identifiers::ChainId;
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::{data_types::ChainInfoQuery, node::ValidatorNode};
use tokio::time::{sleep, Instant};

use super::report::{PerChainTipLag, TipLagReport, TipLagSample, TipLagTrend};

/// Lag delta (in blocks) within which two samples are considered unchanged.
const STABLE_BAND: i64 = 2;

pub async fn run<N, Env>(
    node: &N,
    context: &ClientContext<Env>,
    chains: &[ChainId],
    samples: usize,
    interval: Duration,
) -> Result<TipLagReport>
where
    N: ValidatorNode,
    Env: linera_core::Environment,
{
    let started = Instant::now();
    let mut per_chain: Vec<PerChainTipLag> = chains
        .iter()
        .map(|c| PerChainTipLag {
            chain_id: c.to_string(),
            samples: Vec::with_capacity(samples),
            trend: TipLagTrend::Stable,
        })
        .collect();

    for i in 0..samples.max(1) {
        if i > 0 {
            sleep(interval).await;
        }
        let t_secs = started.elapsed().as_secs();
        for (idx, &chain) in chains.iter().enumerate() {
            let candidate_tip = node
                .handle_chain_info_query(ChainInfoQuery::new(chain))
                .await
                .map(|response| response.info.next_block_height.0)
                .unwrap_or(0);

            // Reference tip: synchronize the local view from the committee and
            // read the resulting next block height.
            let chain_client = context.make_chain_client(chain).await?;
            let reference_tip = chain_client
                .synchronize_chain_state(chain)
                .await
                .map(|info| info.next_block_height.0)
                .unwrap_or(0);

            per_chain[idx].samples.push(TipLagSample {
                t_secs,
                candidate_tip,
                reference_tip,
                lag_blocks: reference_tip as i64 - candidate_tip as i64,
            });
        }
    }

    for pc in &mut per_chain {
        pc.trend = compute_trend(&pc.samples);
    }
    Ok(TipLagReport { per_chain })
}

/// Classify the lag trend by comparing the first and last sample.
pub(super) fn compute_trend(samples: &[TipLagSample]) -> TipLagTrend {
    let (Some(first), Some(last)) = (samples.first(), samples.last()) else {
        return TipLagTrend::Stable;
    };
    if samples.len() < 2 {
        return TipLagTrend::Stable;
    }
    let delta = last.lag_blocks - first.lag_blocks;
    if delta.abs() <= STABLE_BAND {
        TipLagTrend::Stable
    } else if delta < 0 {
        TipLagTrend::Converging
    } else {
        TipLagTrend::Diverging
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(t: u64, lag: i64) -> TipLagSample {
        TipLagSample {
            t_secs: t,
            candidate_tip: 0,
            reference_tip: lag.max(0) as u64,
            lag_blocks: lag,
        }
    }

    #[test]
    fn stable_within_band() {
        assert_eq!(
            compute_trend(&[sample(0, 10), sample(60, 11)]),
            TipLagTrend::Stable
        );
    }

    #[test]
    fn converging_when_lag_shrinks() {
        assert_eq!(
            compute_trend(&[sample(0, 50), sample(60, 10)]),
            TipLagTrend::Converging
        );
    }

    #[test]
    fn diverging_when_lag_grows() {
        assert_eq!(
            compute_trend(&[sample(0, 5), sample(60, 80)]),
            TipLagTrend::Diverging
        );
    }

    #[test]
    fn single_sample_is_stable() {
        assert_eq!(compute_trend(&[sample(0, 10)]), TipLagTrend::Stable);
    }

    #[test]
    fn empty_is_stable() {
        assert_eq!(compute_trend(&[]), TipLagTrend::Stable);
    }
}
