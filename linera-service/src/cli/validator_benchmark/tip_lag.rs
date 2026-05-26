// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L5 tip-lag snapshot.
//!
//! Compares the candidate's reported tip against the network reference tip,
//! taken as the highest `next_block_height` reported by the current committee
//! members. This is a cheap read (one `handle_chain_info_query` per validator
//! per sample), not a full chain synchronization, so it never downloads history
//! or blocks on a slow sync. Repeated sampling reveals whether the candidate is
//! keeping up, catching up, or falling behind.

use std::time::Duration;

use anyhow::Result;
use linera_base::identifiers::ChainId;
use linera_client::client_context::ClientContext;
use linera_core::{
    data_types::ChainInfoQuery,
    node::{ValidatorNode, ValidatorNodeProvider as _},
};
use tokio::time::{sleep, Instant};

use super::{
    progress::Progress,
    report::{PerChainTipLag, TipLagReport, TipLagSample, TipLagTrend},
    rpc::timed,
};

/// Lag delta (in blocks) within which two samples are considered unchanged.
const STABLE_BAND: i64 = 2;

pub async fn run<N, Env>(
    node: &N,
    context: &ClientContext<Env>,
    chains: &[ChainId],
    samples: usize,
    interval: Duration,
    rpc_timeout: Duration,
    progress: &Progress,
) -> Result<TipLagReport>
where
    N: ValidatorNode,
    Env: linera_core::Environment,
{
    let started = Instant::now();
    let phase = progress.phase("L6 tip-lag", Some(samples.max(1) as u64));

    // Build the committee node set once from the wallet's genesis config: it is
    // local (no RPC, no tracked chain needed). Validators serve committee info
    // only to local clients, and the operator's wallet may have no default
    // chain, so this is the robust source. NOTE: it is the genesis committee;
    // members added/removed since genesis are missed/unreachable, which is fine
    // for a max-over-reachable reference tip.
    let committee_nodes = committee_nodes(context);

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
            // Count down by the second so the bar visibly stays alive during the
            // (potentially long) inter-sample wait.
            let mut remaining = interval.as_secs();
            while remaining > 0 {
                phase.set_message(format!("next sample in {remaining}s"));
                sleep(Duration::from_secs(1)).await;
                remaining -= 1;
            }
        }
        phase.set_message(format!("sample {}/{}", i + 1, samples.max(1)));
        let t_secs = started.elapsed().as_secs();
        for (idx, &chain) in chains.iter().enumerate() {
            let candidate_tip = timed(
                rpc_timeout,
                node.handle_chain_info_query(ChainInfoQuery::new(chain)),
            )
            .await
            .map_or(0, |response| response.info.next_block_height.0);

            // Reference tip = highest tip across reachable committee members.
            let mut reference_tip = 0u64;
            for committee_node in &committee_nodes {
                if let Ok(response) = timed(
                    rpc_timeout,
                    committee_node.handle_chain_info_query(ChainInfoQuery::new(chain)),
                )
                .await
                {
                    reference_tip = reference_tip.max(response.info.next_block_height.0);
                }
            }

            // Heights fit in i64 in practice; clamp defensively rather than cast.
            let reference = i64::try_from(reference_tip).unwrap_or(i64::MAX);
            let candidate = i64::try_from(candidate_tip).unwrap_or(i64::MAX);
            per_chain[idx].samples.push(TipLagSample {
                t_secs,
                candidate_tip,
                reference_tip,
                lag_blocks: reference.saturating_sub(candidate),
            });
        }
        phase.inc(1);
    }
    phase.finish_ok();

    for pc in &mut per_chain {
        pc.trend = compute_trend(&pc.samples);
    }
    Ok(TipLagReport { per_chain })
}

/// Build the genesis committee's validator nodes from the wallet's local config.
///
/// Returns the concrete gRPC client type from the context's node provider;
/// unparseable addresses are skipped.
fn committee_nodes<Env>(context: &ClientContext<Env>) -> Vec<linera_rpc::Client>
where
    Env: linera_core::Environment,
{
    let provider = context.make_node_provider();
    context
        .genesis_config
        .committee
        .validators()
        .values()
        .filter_map(|state| provider.make_node(&state.network_address).ok())
        .collect()
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
            reference_tip: u64::try_from(lag).unwrap_or(0),
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
