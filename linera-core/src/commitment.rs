// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A validator's epoch commitments: assembly from the vote ledger, and the agent
//! that commits every revoked epoch the validator belonged to.

use linera_base::{
    crypto::{ValidatorPublicKey, ValidatorSignature},
    data_types::{ArithmeticError, Blob, CommitmentManifest, Epoch, SignedCommitmentManifest},
    identifiers::ChainId,
};
use linera_chain::{
    epoch_commitment::{CommitmentChunk, CommitmentEntry},
    manager::LockingBlock,
    vote_ledger::{JustifiedVote, LedgerEntry, VoteRecord},
};
use linera_execution::ExecutionError;
use linera_storage::Storage;
use linera_views::ViewError;
use thiserror::Error;
use tracing::info;

use crate::{
    data_types::ChainInfoQuery,
    worker::{WorkerError, WorkerState},
};

/// An error while assembling or driving an epoch commitment.
#[derive(Debug, Error)]
pub enum CommitmentError {
    /// An error from the worker, e.g. while assembling or signing.
    #[error(transparent)]
    Worker(#[from] WorkerError),
    /// An error reading from or writing to storage.
    #[error(transparent)]
    View(#[from] ViewError),
    /// An error resolving epochs or committees.
    #[error(transparent)]
    Execution(#[from] ExecutionError),
    /// An epoch number overflowed.
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError),
    /// A serialization error.
    #[error(transparent)]
    Bcs(#[from] bcs::Error),
    /// A shard could not be reached or refused the request.
    #[error("Shard request failed: {0}")]
    Shard(String),
    /// The epoch is revoked, but the admin-chain event naming its committee is not
    /// in local storage yet.
    #[error("The committee for revoked epoch {0} is not available locally yet")]
    MissingCommittee(Epoch),
}

/// Access to all shards of a validator, for the two operations an epoch
/// commitment needs from them: freezing signing, and signing the manifest with
/// the validator key, which lives in the shards.
///
/// A [`WorkerState`] implements this directly for single-worker validators and
/// tests; a multi-shard validator implements it over the internal RPCs.
pub trait ShardControl {
    /// Permanently freezes vote signing in the given epoch and all earlier ones,
    /// on every shard. When this returns, every vote any shard created is durably
    /// in the ledger, and no more can be added.
    async fn freeze_epoch(&self, epoch: Epoch) -> Result<(), CommitmentError>;

    /// Signs the manifest with the validator key. The signer refuses unless the
    /// manifest names its validator and the epoch is frozen.
    async fn sign_commitment_manifest(
        &self,
        manifest: &CommitmentManifest,
    ) -> Result<ValidatorSignature, CommitmentError>;

    /// Returns the chain manager's current locking block, obtained from the
    /// chain worker responsible for the chain — only that worker may access the
    /// chain's state.
    async fn locking_block(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<LockingBlock>, CommitmentError>;
}

impl<S> ShardControl for WorkerState<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn freeze_epoch(&self, epoch: Epoch) -> Result<(), CommitmentError> {
        Ok(WorkerState::freeze_epoch(self, epoch).await?)
    }

    async fn sign_commitment_manifest(
        &self,
        manifest: &CommitmentManifest,
    ) -> Result<ValidatorSignature, CommitmentError> {
        Ok(WorkerState::sign_commitment_manifest(self, manifest).await?)
    }

    async fn locking_block(
        &self,
        chain_id: ChainId,
    ) -> Result<Option<LockingBlock>, CommitmentError> {
        let query = ChainInfoQuery::new(chain_id).with_manager_values();
        let response = self.handle_chain_info_query(query).await?;
        Ok(response.info.manager.requested_locking.map(|boxed| *boxed))
    }
}

/// Drives this validator's epoch commitments: when an epoch the validator
/// belonged to is revoked, freezes it on all shards, assembles the commitment
/// from the vote ledger, has a shard sign the manifest, and persists the signed
/// commitment for publication on the admin chain.
pub struct CommitmentAgent<S, C> {
    storage: S,
    shards: C,
    validator: ValidatorPublicKey,
    max_chunk_bytes: usize,
    /// The first epoch the next pass will look at; every epoch below it is
    /// already handled. Starts over from zero on restart — handled epochs are
    /// then skipped by their stored commitment or non-membership.
    next_epoch: Epoch,
}

impl<S, C> CommitmentAgent<S, C>
where
    S: Storage,
    C: ShardControl,
{
    /// Creates an agent committing in the given validator's name. Commitment
    /// blobs are capped at `max_chunk_bytes` or the committee policy's maximum
    /// blob size, whichever is lower.
    pub fn new(
        storage: S,
        shards: C,
        validator: ValidatorPublicKey,
        max_chunk_bytes: usize,
    ) -> Self {
        CommitmentAgent {
            storage,
            shards,
            validator,
            max_chunk_bytes,
            next_epoch: Epoch::ZERO,
        }
    }

    /// Commits every revoked epoch that is not handled yet, in epoch order, and
    /// returns the newly committed epochs. Call this periodically; it is
    /// idempotent, and a run interrupted mid-epoch redoes that epoch on the next
    /// call — the stored signed manifest is what marks an epoch as done.
    pub async fn process_revoked_epochs(&mut self) -> Result<Vec<Epoch>, CommitmentError> {
        let mut committed = Vec::new();
        while self.storage.is_epoch_revoked(self.next_epoch).await? {
            let epoch = self.next_epoch;
            if self.storage.read_pending_commitment(epoch).await?.is_none()
                && self.commit_epoch(epoch).await?
            {
                committed.push(epoch);
            }
            self.next_epoch = epoch.try_add_one()?;
        }
        Ok(committed)
    }

    /// Commits the given revoked epoch, unless this validator was not in its
    /// committee. Returns whether a commitment was made.
    async fn commit_epoch(&self, epoch: Epoch) -> Result<bool, CommitmentError> {
        let committee = self
            .storage
            .committee_for_epoch(epoch)
            .await?
            .ok_or(CommitmentError::MissingCommittee(epoch))?;
        if !committee.validators().contains_key(&self.validator) {
            return Ok(false);
        }
        // Chunks must satisfy the blob size limit at publication time; the margin
        // leaves room for the chunk's own length prefix.
        let policy_chunk_bytes = usize::try_from(committee.policy().maximum_blob_size)
            .unwrap_or(usize::MAX)
            .saturating_sub(16);
        let max_chunk_bytes = self.max_chunk_bytes.min(policy_chunk_bytes);
        self.shards.freeze_epoch(epoch).await?;
        let assembled = assemble_commitment(
            &self.storage,
            &self.shards,
            epoch,
            self.validator,
            max_chunk_bytes,
        )
        .await?;
        let signature = self
            .shards
            .sign_commitment_manifest(&assembled.manifest)
            .await?;
        let signed = SignedCommitmentManifest {
            manifest: assembled.manifest,
            signature,
        };
        // The blobs go in before the manifest: the stored manifest marks the
        // epoch as done, so everything it references must already be durable.
        self.storage.write_blobs(&assembled.blobs).await?;
        self.storage.write_pending_commitment(&signed).await?;
        info!(
            epoch = %epoch,
            chunks = signed.manifest.blob_hashes.len(),
            "committed to this validator's votes in the revoked epoch"
        );
        Ok(true)
    }
}

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
/// latest vote from being bypassed unrecorded while we read. Chain state is never
/// read from storage directly — a pending vote's locking certificate is obtained
/// through the chain's worker, via [`ShardControl::locking_block`].
pub async fn assemble_commitment<S: Storage, C: ShardControl>(
    storage: &S,
    shards: &C,
    epoch: Epoch,
    validator: ValidatorPublicKey,
    max_chunk_bytes: usize,
) -> Result<AssembledCommitment, CommitmentError> {
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
        let entry = commitment_entry(storage, shards, chain_id, ledger_entry).await?;
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
        .collect::<Result<Vec<_>, CommitmentError>>()?;
    let manifest = CommitmentManifest {
        epoch,
        validator,
        blob_hashes: blobs.iter().map(|blob| blob.id().hash).collect(),
    };
    Ok(AssembledCommitment { blobs, manifest })
}

/// Builds the commitment entry for one chain: the ledger entry with the latest
/// vote's justification recovered.
async fn commitment_entry<S: Storage, C: ShardControl>(
    storage: &S,
    shards: &C,
    chain_id: ChainId,
    ledger_entry: LedgerEntry,
) -> Result<CommitmentEntry, CommitmentError> {
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
                Some(recover_justification(storage, shards, chain_id, &latest).await?)
            }
        }
    };
    if justification.is_none() && latest.justification_commitment.is_some() {
        return Err(WorkerError::MissingVoteJustification(chain_id).into());
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
async fn recover_justification<S: Storage, C: ShardControl>(
    storage: &S,
    shards: &C,
    chain_id: ChainId,
    latest: &VoteRecord,
) -> Result<linera_chain::types::ValidatedBlockCertificate, CommitmentError> {
    let commitment = latest
        .justification_commitment
        .expect("only called for votes that cited a quorum");
    if let Some(LockingBlock::Regular(certificate)) = shards.locking_block(chain_id).await? {
        if certificate.full_justification_commitment() == commitment {
            return Ok(certificate);
        }
    }
    if let Some(certificate) = storage.read_certificate(latest.block_hash).await? {
        if let Some(cited) = certificate.cited_validated_certificate(commitment) {
            return Ok(cited);
        }
    }
    Err(WorkerError::MissingVoteJustification(chain_id).into())
}
