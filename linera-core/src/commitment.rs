// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Assembly of a validator's epoch commitment from its vote ledger.

use linera_base::{
    crypto::ValidatorPublicKey,
    data_types::{Blob, Epoch},
    identifiers::ChainId,
};
use linera_chain::{
    epoch_commitment::{CommitmentChunk, CommitmentEntry, CommitmentManifest},
    manager::LockingBlock,
    vote_ledger::{JustifiedVote, LedgerEntry, VoteRecord},
};
use linera_storage::Storage;

use crate::worker::WorkerError;

/// A commitment assembled from the vote ledger, ready to be signed.
pub struct AssembledCommitment {
    /// The blobs holding the commitment's chunks.
    pub blobs: Vec<Blob>,
    /// The manifest listing them.
    pub manifest: CommitmentManifest,
}

/// Assembles this validator's commitment for the given epoch from the vote
/// ledger, chunked into blobs of at most roughly `max_chunk_bytes` each (a single
/// oversized entry still becomes its own chunk).
///
/// This must only run after the epoch has been frozen on every worker sharing the
/// storage: the freeze is what makes the ledger complete, and it keeps a pending
/// latest vote from being bypassed unrecorded while we read.
pub async fn assemble_commitment<S: Storage>(
    storage: &S,
    epoch: Epoch,
    validator: ValidatorPublicKey,
    max_chunk_bytes: usize,
) -> Result<AssembledCommitment, WorkerError> {
    let mut chunks = Vec::new();
    let mut entries = Vec::new();
    let mut entries_bytes = 0;
    // The chain IDs are sorted by their serialized form, so the entries end up
    // sorted across the whole commitment.
    for chain_id in storage.vote_ledger_chain_ids(epoch).await? {
        let ledger_entry = storage
            .read_vote_ledger_entry(epoch, chain_id)
            .await?
            .ok_or(WorkerError::MissingVoteJustification(chain_id))?;
        let entry = commitment_entry(storage, chain_id, ledger_entry).await?;
        let entry_bytes = bcs::serialized_size(&entry)?;
        if !entries.is_empty() && entries_bytes + entry_bytes > max_chunk_bytes {
            chunks.push(CommitmentChunk {
                entries: std::mem::take(&mut entries),
            });
            entries_bytes = 0;
        }
        entries_bytes += entry_bytes;
        entries.push(entry);
    }
    if !entries.is_empty() {
        chunks.push(CommitmentChunk { entries });
    }
    let blobs = chunks
        .iter()
        .map(|chunk| Ok(Blob::new_epoch_commitment(bcs::to_bytes(chunk)?)))
        .collect::<Result<Vec<_>, bcs::Error>>()?;
    let manifest = CommitmentManifest {
        epoch,
        validator,
        blob_hashes: blobs.iter().map(|blob| blob.id().hash).collect(),
    };
    Ok(AssembledCommitment { blobs, manifest })
}

/// Builds the commitment entry for one chain: the ledger entry with the latest
/// vote's justification recovered.
async fn commitment_entry<S: Storage>(
    storage: &S,
    chain_id: ChainId,
    ledger_entry: LedgerEntry,
) -> Result<CommitmentEntry, WorkerError> {
    let LedgerEntry {
        latest,
        mut superseded,
    } = ledger_entry;
    let justification = match latest.justification_commitment {
        None => None, // The vote cited nothing (fast or first round).
        Some(_) => {
            // If the vote's height was passed while the certificate could not
            // reproduce the cited quorum, the pair was recorded alongside the
            // superseded votes; it is the committed vote, not a superseded one.
            if let Some(index) = superseded.iter().position(|vote| vote.record == latest) {
                superseded.remove(index).justification
            } else {
                Some(recover_justification(storage, chain_id, &latest).await?)
            }
        }
    };
    if justification.is_none() && latest.justification_commitment.is_some() {
        return Err(WorkerError::MissingVoteJustification(chain_id));
    }
    Ok(CommitmentEntry {
        chain_id,
        committed: JustifiedVote {
            record: latest,
            justification,
        },
        superseded,
    })
}

/// Recovers the validated certificate the latest vote cited: from the chain
/// manager's locking certificate if the vote is still pending at the tip, or from
/// the stored confirmed certificate's justification chain if the voted block was
/// finalized.
async fn recover_justification<S: Storage>(
    storage: &S,
    chain_id: ChainId,
    latest: &VoteRecord,
) -> Result<linera_chain::types::ValidatedBlockCertificate, WorkerError> {
    let commitment = latest
        .justification_commitment
        .expect("only called for votes that cited a quorum");
    let chain = storage.load_chain(chain_id).await?;
    if let Some(LockingBlock::Regular(certificate)) = chain.manager.locking_block.get() {
        if certificate.full_justification_commitment() == commitment {
            return Ok(certificate.clone());
        }
    }
    if let Some(certificate) = storage.read_certificate(latest.block_hash).await? {
        if let Some(cited) = certificate.cited_validated_certificate(commitment) {
            return Ok(cited);
        }
    }
    Err(WorkerError::MissingVoteJustification(chain_id))
}
