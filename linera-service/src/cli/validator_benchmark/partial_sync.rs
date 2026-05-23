// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L6 partial sync (opt-in `--deep`): bounded write-path ingest measurement.
//!
//! Pushes at most `max_blocks` confirmed certificates starting at the
//! candidate's current tip, mirroring the client-side push that
//! `ChainClient::sync_validator` performs (including blob upload on
//! `BlobsNotFound`) but capped to a fixed number of blocks. This is the only
//! layer with a stateful side effect on the candidate, hence opt-in.

use std::time::{Duration, Instant};

use anyhow::Result;
use linera_base::{data_types::BlockHeight, identifiers::ChainId};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::node::{CrossChainMessageDelivery, NodeError, ValidatorNode};
use linera_storage::Storage as _;

use super::{progress::Progress, report::PartialSyncReport, rpc::timed};

/// Compute the exclusive end height for a bounded sync, saturating on overflow.
pub(super) fn end_height(candidate_tip: u64, max_blocks: u32, local_tip: u64) -> u64 {
    candidate_tip
        .saturating_add(max_blocks as u64)
        .min(local_tip)
}

pub async fn run<N, Env>(
    node: &N,
    context: &ClientContext<Env>,
    chain: ChainId,
    max_blocks: u32,
    rpc_timeout: Duration,
    progress: &Progress,
) -> Result<PartialSyncReport>
where
    N: ValidatorNode,
    Env: linera_core::Environment,
{
    let mut report = PartialSyncReport {
        chain_id: chain.to_string(),
        from_height: 0,
        to_height: 0,
        blocks_attempted: 0,
        blocks_accepted: 0,
        bytes_in: 0,
        duration_secs: 0.0,
        blocks_per_sec: 0.0,
    };

    let chain_client = context.make_chain_client(chain).await?;

    // The local sync can be heavy (it downloads the certificates to push); give
    // it a visible spinner so it never looks frozen. NOTE: on a tall chain the
    // wallet does not track, this can exceed --rpc-timeout-secs; raise it or use
    // a tracked chain if L6 times out here.
    let sync_phase = progress.phase("L6 sync local state", None);
    let local_tip = match timed(rpc_timeout, chain_client.synchronize_chain_state(chain)).await {
        Ok(info) => {
            sync_phase.finish_ok();
            info.next_block_height.0
        }
        Err(category) => {
            sync_phase.finish_fail();
            anyhow::bail!("L6 local sync failed ({category})");
        }
    };

    // The candidate may not yet know the chain at all (BlobsNotFound -> 0).
    let candidate_tip = match timed(
        rpc_timeout,
        node.handle_chain_info_query(linera_core::data_types::ChainInfoQuery::new(chain)),
    )
    .await
    {
        Ok(response) => response.info.next_block_height.0,
        Err(_) => 0,
    };

    let from = candidate_tip;
    let to = end_height(candidate_tip, max_blocks, local_tip);
    report.from_height = from;
    report.to_height = to;

    if to <= from {
        return Ok(report);
    }

    let phase = progress.phase("L6 partial sync", Some(to - from));
    let heights: Vec<BlockHeight> = (from..to).map(BlockHeight).collect();
    let storage = chain_client.storage_client();
    // Certificates and blobs stay wrapped in the storage cache `Arc`, which is
    // exactly what the validator node's handlers accept.
    let certificates = storage
        .read_certificates_by_heights(chain, &heights)
        .await?
        .into_iter()
        .flatten();

    let started = Instant::now();
    for certificate in certificates {
        report.blocks_attempted += 1;
        phase.inc(1);
        report.bytes_in += bcs::serialized_size(&*certificate).unwrap_or(0) as u64;

        // First attempt. A timeout or non-blob error skips this block (the run
        // keeps going); a `BlobsNotFound` triggers the blob-upload retry below.
        // The typed error is needed here, so use a raw timeout rather than the
        // string-categorizing `timed` helper.
        let first = tokio::time::timeout(
            rpc_timeout,
            node.handle_confirmed_certificate(
                certificate.clone(),
                CrossChainMessageDelivery::NonBlocking,
            ),
        )
        .await;
        let missing = match first {
            Ok(Ok(_)) => {
                report.blocks_accepted += 1;
                continue;
            }
            Ok(Err(NodeError::BlobsNotFound(missing_blob_ids))) => missing_blob_ids,
            Ok(Err(_)) | Err(_) => continue,
        };

        let blobs = storage
            .read_blobs(&missing)
            .await?
            .into_iter()
            .flatten()
            .map(|b| b.into_std())
            .collect::<Vec<_>>();
        if timed(rpc_timeout, node.upload_blobs(blobs)).await.is_err() {
            continue;
        }
        if timed(
            rpc_timeout,
            node.handle_confirmed_certificate(certificate, CrossChainMessageDelivery::NonBlocking),
        )
        .await
        .is_ok()
        {
            report.blocks_accepted += 1;
        }
    }

    let duration = started.elapsed().as_secs_f64().max(f64::EPSILON);
    report.duration_secs = duration;
    report.blocks_per_sec = report.blocks_accepted as f64 / duration;
    phase.finish_ok();
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::end_height;

    #[test]
    fn caps_at_max_blocks() {
        assert_eq!(end_height(1_000, 500, 10_000), 1_500);
    }

    #[test]
    fn caps_at_local_tip() {
        assert_eq!(end_height(1_000, 5_000, 1_200), 1_200);
    }

    #[test]
    fn saturates_on_overflow() {
        assert_eq!(end_height(u64::MAX - 1, 1_000, u64::MAX), u64::MAX);
    }

    #[test]
    fn nothing_to_do_when_caught_up() {
        assert_eq!(end_height(1_200, 1_000, 1_200), 1_200);
    }
}
