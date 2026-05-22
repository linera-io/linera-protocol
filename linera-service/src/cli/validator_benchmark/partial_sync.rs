// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! L6 partial sync (opt-in `--deep`): bounded write-path ingest measurement.
//!
//! Pushes at most `max_blocks` confirmed certificates starting at the
//! candidate's current tip, mirroring the client-side push that
//! `ChainClient::sync_validator` performs (including blob upload on
//! `BlobsNotFound`) but capped to a fixed number of blocks. This is the only
//! layer with a stateful side effect on the candidate, hence opt-in.

use std::{sync::Arc, time::Instant};

use anyhow::Result;
use linera_base::{data_types::BlockHeight, identifiers::ChainId};
use linera_client::{chain_listener::ClientContext as _, client_context::ClientContext};
use linera_core::node::{CrossChainMessageDelivery, NodeError, ValidatorNode};
use linera_storage::Storage as _;

use super::report::PartialSyncReport;

/// Compute the exclusive end height for a bounded sync, saturating on overflow.
pub(super) fn end_height(candidate_tip: u64, max_blocks: u32, local_tip: u64) -> u64 {
    candidate_tip.saturating_add(max_blocks as u64).min(local_tip)
}

pub async fn run<N, Env>(
    node: &N,
    context: &ClientContext<Env>,
    chain: ChainId,
    max_blocks: u32,
) -> Result<PartialSyncReport>
where
    N: ValidatorNode,
    Env: linera_core::Environment,
{
    let chain_client = context.make_chain_client(chain).await?;

    // Make sure the local node holds the certificates we are about to push.
    let local_info = chain_client.synchronize_chain_state(chain).await?;
    let local_tip = local_info.next_block_height.0;

    // The candidate may not yet know the chain at all (BlobsNotFound -> 0).
    let candidate_tip = match node
        .handle_chain_info_query(linera_core::data_types::ChainInfoQuery::new(chain))
        .await
    {
        Ok(response) => response.info.next_block_height.0,
        Err(NodeError::BlobsNotFound(_)) => 0,
        Err(e) => return Err(e.into()),
    };

    let from = candidate_tip;
    let to = end_height(candidate_tip, max_blocks, local_tip);

    let mut report = PartialSyncReport {
        chain_id: chain.to_string(),
        from_height: from,
        to_height: to,
        blocks_attempted: 0,
        blocks_accepted: 0,
        bytes_in: 0,
        duration_secs: 0.0,
        blocks_per_sec: 0.0,
    };

    if to <= from {
        return Ok(report);
    }

    let heights: Vec<BlockHeight> = (from..to).map(BlockHeight).collect();
    let storage = chain_client.storage_client();
    let certificates = storage
        .read_certificates_by_heights(chain, &heights)
        .await?
        .into_iter()
        .flatten()
        .map(Arc::unwrap_or_clone);

    let started = Instant::now();
    for certificate in certificates {
        report.blocks_attempted += 1;
        report.bytes_in += bcs::serialized_size(&certificate).unwrap_or(0) as u64;

        // First attempt; on missing blobs, upload them and retry once.
        let missing = match node
            .handle_confirmed_certificate(
                certificate.clone(),
                CrossChainMessageDelivery::NonBlocking,
            )
            .await
        {
            Ok(_) => {
                report.blocks_accepted += 1;
                continue;
            }
            Err(NodeError::BlobsNotFound(missing_blob_ids)) => missing_blob_ids,
            Err(e) => return Err(e.into()),
        };

        let blobs = storage
            .read_blobs(&missing)
            .await?
            .into_iter()
            .flatten()
            .map(Arc::unwrap_or_clone)
            .collect();
        node.upload_blobs(blobs).await?;
        node.handle_confirmed_certificate(certificate, CrossChainMessageDelivery::NonBlocking)
            .await?;
        report.blocks_accepted += 1;
    }

    let duration = started.elapsed().as_secs_f64().max(f64::EPSILON);
    report.duration_secs = duration;
    report.blocks_per_sec = report.blocks_accepted as f64 / duration;
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
