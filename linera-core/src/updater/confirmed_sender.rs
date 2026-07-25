// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! A lightweight, reusable primitive for pushing confirmed-block certificates to a single
//! validator.
//!
//! [`ConfirmedCertificateSender`] extracts the confirmed-certificate synchronization path out of
//! [`crate::updater::ValidatorUpdater`] so it can be reused by clients that do not construct a
//! full [`crate::client::Client`] or [`crate::local_node::LocalNodeClient`] (e.g. the block
//! exporter). It operates directly on a [`Storage`] handle, a [`RemoteNode`], and the admin chain
//! id, reusing the existing dependency-upload logic (missing blobs, missing epoch/committee events
//! via the admin chain, and ancestor certificates).

use std::collections::{BTreeMap, BTreeSet};

use futures::future;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlockHeight},
    identifiers::{BlobId, BlobType, ChainId, StreamId},
};
use linera_chain::types::{ConfirmedBlock, GenericCertificate};
use linera_execution::{system::EPOCH_STREAM_NAME, BlobState};
use linera_storage::{Arc as CacheArc, ResultReadCertificates, Storage};
use tracing::{instrument, Level};

use crate::{
    client::chain_client,
    data_types::{ChainInfo, ChainInfoQuery},
    node::{CrossChainMessageDelivery, NodeError, ValidatorNode},
    remote_node::RemoteNode,
    LocalNodeError,
};

/// Answers the few chain-level queries that cannot be served from the certificate, blob, and
/// height-index partitions.
///
/// Those partitions can be read directly, but a chain's own state view is owned by the chain worker
/// while it is running: loading it elsewhere races with the worker and can corrupt data. Consumers
/// therefore supply their own way to answer these queries — the client routes them through its
/// local worker.
pub trait LocalChainState {
    /// Returns the next block height of the given chain, according to local state.
    async fn next_block_height(
        &self,
        chain_id: ChainId,
    ) -> Result<BlockHeight, chain_client::Error>;

    /// Returns the hashes of the given chain's blocks at the given heights.
    ///
    /// Heights the chain does not have are omitted, so the result may be shorter than `heights`.
    async fn block_hashes(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<CryptoHash>, chain_client::Error>;
}

/// Pushes confirmed-block certificates to a single validator, uploading their dependencies as
/// needed.
///
/// This is the confirmed-certificate synchronization path shared by the client's
/// `ValidatorUpdater` and by out-of-crate consumers such as the block exporter.
/// Certificates and blobs are read from `storage` directly, and the remaining chain-level queries
/// go through [`LocalChainState`], so this requires neither a wallet nor a signer.
///
/// A confirmed certificate can be rejected by a validator that is missing one of the certificate's
/// dependencies. This type reproduces the client's recovery logic:
/// - `EventsNotFound` for the admin chain's epoch stream means the validator does not yet know the
///   committee that signed the certificate, so the admin chain's confirmed blocks are pushed.
/// - `BlobsNotFound` means the validator is missing blobs the certificate requires, which are read
///   from `storage` and uploaded.
///
/// The certificate is then retried.
pub struct ConfirmedCertificateSender<S, N, L> {
    storage: S,
    remote_node: RemoteNode<N>,
    local_chain_state: L,
    admin_chain_id: ChainId,
    certificate_upload_batch_size: u64,
    backfill_height_indices: bool,
}

impl<S, N, L> Clone for ConfirmedCertificateSender<S, N, L>
where
    S: Clone,
    N: Clone,
    L: Clone,
{
    fn clone(&self) -> Self {
        ConfirmedCertificateSender {
            storage: self.storage.clone(),
            remote_node: self.remote_node.clone(),
            local_chain_state: self.local_chain_state.clone(),
            admin_chain_id: self.admin_chain_id,
            certificate_upload_batch_size: self.certificate_upload_batch_size,
            backfill_height_indices: self.backfill_height_indices,
        }
    }
}

impl<S, N, L> ConfirmedCertificateSender<S, N, L>
where
    S: Storage + Clone,
    N: ValidatorNode + Clone,
    L: LocalChainState + Clone,
{
    /// Creates a new sender targeting `remote_node`, reading confirmed blocks and their
    /// dependencies from `storage`.
    ///
    /// `admin_chain_id` identifies the chain whose epoch stream carries committee events.
    /// `certificate_upload_batch_size` bounds how many certificates are read and pushed per batch
    /// during height synchronization.
    pub fn new(
        storage: S,
        remote_node: RemoteNode<N>,
        local_chain_state: L,
        admin_chain_id: ChainId,
        certificate_upload_batch_size: u64,
    ) -> Self {
        ConfirmedCertificateSender {
            storage,
            remote_node,
            local_chain_state,
            admin_chain_id,
            certificate_upload_batch_size,
            backfill_height_indices: false,
        }
    }

    /// Enables writing `(chain_id, height) -> hash` indices back to storage for certificates that
    /// were only found through the block-hash-list fallback.
    ///
    /// This is meant for consumers that own the storage they read from. Consumers that merely read
    /// another component's storage, such as the block exporter, must leave it disabled.
    pub(crate) fn with_height_index_backfill(mut self) -> Self {
        self.backfill_height_indices = true;
        self
    }

    /// Synchronizes the validator to `target_next_block_height` by sending the confirmed
    /// certificates held in storage for the chain, and returns the validator's [`ChainInfo`]
    /// afterwards.
    ///
    /// This is the height-synchronization phase: it sends the certificates in storage for the range
    /// `[validator_next_height, target_next_block_height)`, in order. Only heights actually present
    /// in storage are sent; missing heights are silently skipped. This is what makes the "leave gaps on the validator
    /// side" behavior (#4181) work: a chain we merely *receive* from is stored only at its
    /// message-bearing heights, so exactly those are pushed. The validator executes the contiguous
    /// prefix and preprocesses any block that sits above a gap.
    ///
    /// For an existing chain (`target_next_block_height > 0`) it optimistically sends the last
    /// certificate first, then any earlier locally-held certificates in order. For a new chain
    /// (`target_next_block_height == 0`) it sends the chain description and dependencies first, then
    /// queries the validator's state.
    #[instrument(level = "debug", skip_all, fields(%chain_id))]
    pub async fn send_confirmed_chain(
        &mut self,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
        delivery: CrossChainMessageDelivery,
        latest_certificate: Option<CacheArc<GenericCertificate<ConfirmedBlock>>>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        if target_next_block_height.0 > 0 {
            self.sync_chain_height(
                chain_id,
                target_next_block_height,
                delivery,
                latest_certificate,
            )
            .await
        } else {
            self.initialize_new_chain_on_validator(chain_id).await
        }
    }

    /// Sends chain information for all chains referenced by the given blobs.
    ///
    /// Reads blob states from storage, determines the specific chain heights needed, and sends the
    /// confirmed certificates at exactly those heights. With sparse chains, this only sends the
    /// specific blocks containing the blobs, not all blocks up to those heights.
    pub(crate) async fn send_chain_info_for_blobs(
        &self,
        blob_ids: &[BlobId],
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), chain_client::Error> {
        let blob_states = self.read_blob_states_from_storage(blob_ids).await?;

        let mut chain_heights: BTreeMap<ChainId, BTreeSet<BlockHeight>> = BTreeMap::new();
        for blob_state in blob_states {
            chain_heights
                .entry(blob_state.chain_id)
                .or_default()
                .insert(blob_state.block_height);
        }

        self.send_chain_info_at_heights(chain_heights, delivery)
            .await
    }

    /// Sends a single confirmed certificate to the validator, uploading its missing dependencies
    /// (admin-chain committee events, blobs) and retrying as needed.
    #[instrument(
        level = "trace", skip_all, err(level = Level::DEBUG),
        fields(chain_id = %certificate.block().header.chain_id)
    )]
    async fn send_confirmed_certificate(
        &mut self,
        certificate: &CacheArc<GenericCertificate<ConfirmedBlock>>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let mut result = self
            .remote_node
            .handle_optimized_confirmed_certificate(certificate, delivery)
            .await;

        let mut sent_admin_chain = false;
        let mut sent_blobs = false;
        loop {
            match result {
                Err(NodeError::EventsNotFound(event_ids))
                    if !sent_admin_chain
                        && certificate.inner().chain_id() != self.admin_chain_id
                        && event_ids.iter().all(|event_id| {
                            event_id.stream_id == StreamId::system(EPOCH_STREAM_NAME)
                                && event_id.chain_id == self.admin_chain_id
                        }) =>
                {
                    // The validator doesn't have the committee that signed the certificate.
                    self.update_admin_chain().await?;
                    sent_admin_chain = true;
                }
                Err(NodeError::BlobsNotFound(blob_ids)) if !sent_blobs => {
                    // The validator is missing the blobs required by the certificate.
                    self.remote_node
                        .check_blobs_not_found(certificate, &blob_ids)?;
                    // The certificate is confirmed, so the blobs must be in storage.
                    let maybe_blobs: Option<Vec<CacheArc<Blob>>> = self
                        .storage
                        .read_blobs(&blob_ids)
                        .await
                        .map_err(LocalNodeError::from)?
                        .into_iter()
                        .collect();
                    let blobs = maybe_blobs.ok_or(NodeError::BlobsNotFound(blob_ids))?;
                    self.remote_node
                        .node
                        .upload_blobs(blobs.into_iter().map(|blob| blob.into_std()).collect())
                        .await?;
                    sent_blobs = true;
                }
                result => {
                    if let Err(err) = &result {
                        self.warn_if_unexpected(err);
                    }
                    return Ok(result?);
                }
            }
            result = self
                .remote_node
                .handle_confirmed_certificate(certificate.clone(), delivery)
                .await;
        }
    }

    /// Pushes the admin chain's confirmed blocks to the validator, up to the height we hold in
    /// storage.
    ///
    /// Called when the validator reports that it is missing the committee that signed a
    /// certificate. Unlike the client's `ValidatorUpdater`, this performs only height
    /// synchronization (no consensus-round synchronization), which is all that is required to
    /// deliver the confirmed committee events and all that the storage-only primitive can do
    /// without a local worker.
    async fn update_admin_chain(&mut self) -> Result<(), chain_client::Error> {
        let admin_next_block_height = self
            .local_chain_state
            .next_block_height(self.admin_chain_id)
            .await?;
        Box::pin(self.send_confirmed_chain(
            self.admin_chain_id,
            admin_next_block_height,
            CrossChainMessageDelivery::NonBlocking,
            None,
        ))
        .await?;
        Ok(())
    }

    /// Synchronizes a validator to a specific block height by sending the certificates held in
    /// storage.
    ///
    /// Uses an optimistic approach: sends the last certificate first, then, based on the validator's
    /// reported height, sends the earlier certificates in the range. Only the heights actually
    /// present in storage are sent — any that are missing are silently skipped rather than treated
    /// as an error, which is what leaves genuine gaps on the validator (see
    /// [`Self::send_confirmed_chain`] for why that is both safe and intended).
    ///
    /// Returns the [`ChainInfo`] from the validator after synchronization.
    async fn sync_chain_height(
        &mut self,
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
        delivery: CrossChainMessageDelivery,
        latest_certificate: Option<CacheArc<GenericCertificate<ConfirmedBlock>>>,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        let height = target_next_block_height.try_sub_one()?;

        // Get the certificate for the last block we want to send.
        let certificate = if let Some(certificate) = latest_certificate {
            certificate
        } else {
            self.read_certificates_for_heights(chain_id, vec![height])
                .await?
                .into_iter()
                .next()
                .ok_or_else(|| {
                    chain_client::Error::InternalError(
                        "failed to read latest certificate for height sync",
                    )
                })?
        };

        // Optimistically try sending just the last certificate.
        let info = match self
            .send_confirmed_certificate(&certificate, delivery)
            .await
        {
            Ok(info) => info,
            Err(error) => {
                tracing::debug!(
                    address = self.remote_node.address(), %error,
                    "validator failed to handle confirmed certificate; sending whole chain",
                );
                let query = ChainInfoQuery::new(chain_id);
                self.remote_node.handle_chain_info_query(query).await?
            }
        };

        // Calculate which block heights the validator is still missing.
        let heights: Vec<_> = (info.next_block_height.0..target_next_block_height.0)
            .map(BlockHeight)
            .collect();

        if heights.is_empty() {
            return Ok(info);
        }

        let batch_size = self.certificate_upload_batch_size as usize;
        for chunk in heights.chunks(batch_size) {
            let certificates = self
                .read_certificates_for_heights(chain_id, chunk.to_vec())
                .await?;

            for certificate in certificates {
                self.send_confirmed_certificate(&certificate, delivery)
                    .await?;
            }
        }

        Ok(info)
    }

    /// Reads confirmed certificates for the given heights directly from storage, by height.
    ///
    /// Heights that are not present are silently dropped: the returned vector contains only the
    /// certificates actually in storage, so callers naturally skip any block that was never stored
    /// (e.g. a sender's non-message-bearing blocks). Callers must not assume the result covers every
    /// requested height.
    ///
    /// First attempts the direct `(chain_id, height) -> hash` index, then falls back to the chain's
    /// block-hash list. The fallback covers certificates that were persisted before that index was
    /// introduced, which the direct read cannot see.
    ///
    /// Height indices discovered through the fallback are written back to storage only if
    /// [`Self::with_height_index_backfill`] was enabled, so that a consumer reading another
    /// component's storage never writes to it.
    async fn read_certificates_for_heights(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<CacheArc<GenericCertificate<ConfirmedBlock>>>, chain_client::Error> {
        let certificates_by_height = self
            .storage
            .read_certificates_by_heights(chain_id, &heights)
            .await?;

        let all_found = certificates_by_height.len() == heights.len()
            && certificates_by_height
                .iter()
                .all(|certificate| certificate.is_some());

        if all_found {
            return Ok(certificates_by_height.into_iter().flatten().collect());
        }

        let hashes = self
            .local_chain_state
            .block_hashes(chain_id, heights.clone())
            .await?;

        let certificates = self.storage.read_certificates(&hashes).await?;

        match ResultReadCertificates::new(certificates, hashes.clone()) {
            ResultReadCertificates::Certificates(certificates) => {
                if self.backfill_height_indices {
                    let indices: Vec<_> = heights.into_iter().zip(hashes).collect();
                    self.storage
                        .write_certificate_height_indices(chain_id, &indices)
                        .await?;
                }
                Ok(certificates
                    .into_iter()
                    .map(|certificate| self.storage.cache_certificate(certificate))
                    .collect())
            }
            ResultReadCertificates::InvalidHashes(hashes) => {
                Err(chain_client::Error::ReadCertificatesError(hashes))
            }
        }
    }

    /// Initializes a new chain on the validator by sending the chain description and dependencies.
    ///
    /// This is called when the validator doesn't know about the chain yet. Returns the
    /// [`ChainInfo`] from the validator after initialization.
    async fn initialize_new_chain_on_validator(
        &self,
        chain_id: ChainId,
    ) -> Result<Box<ChainInfo>, chain_client::Error> {
        // Send chain description and all dependency chains.
        self.send_chain_info_for_blobs(
            &[BlobId::new(chain_id.0, BlobType::ChainDescription)],
            CrossChainMessageDelivery::NonBlocking,
        )
        .await?;

        // Query the validator's state for this chain.
        let query = ChainInfoQuery::new(chain_id);
        let info = self.remote_node.handle_chain_info_query(query).await?;
        Ok(info)
    }

    /// Reads blob states for the given blobs from storage, failing if any is missing.
    async fn read_blob_states_from_storage(
        &self,
        blob_ids: &[BlobId],
    ) -> Result<Vec<BlobState>, chain_client::Error> {
        let mut blobs_not_found = Vec::new();
        let mut blob_states = Vec::new();
        for (blob_state, blob_id) in self
            .storage
            .read_blob_states(blob_ids)
            .await
            .map_err(LocalNodeError::from)?
            .into_iter()
            .zip(blob_ids)
        {
            match blob_state {
                None => blobs_not_found.push(*blob_id),
                Some(blob_state) => blob_states.push(blob_state),
            }
        }
        if !blobs_not_found.is_empty() {
            return Err(LocalNodeError::BlobsNotFound(blobs_not_found).into());
        }
        Ok(blob_states)
    }

    /// Sends the blocks at exactly the specified heights on multiple chains.
    ///
    /// Unlike a "send up to height" sync, this sends *only* the blocks at the given heights, not the
    /// locally-held prefix leading up to them. Use it only when the required blocks are fully
    /// self-describing to the validator — e.g. bringing over the specific blocks that carry a set of
    /// blobs.
    async fn send_chain_info_at_heights(
        &self,
        chain_heights: impl IntoIterator<Item = (ChainId, BTreeSet<BlockHeight>)>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<(), chain_client::Error> {
        future::try_join_all(chain_heights.into_iter().map(|(chain_id, heights)| {
            let mut sender = self.clone();
            async move {
                // Get all certificates for this chain at the specified heights in one call.
                let heights = heights.into_iter().collect::<Vec<_>>();
                let certificates = sender
                    .storage
                    .read_certificates_by_heights(chain_id, &heights)
                    .await?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();

                // Send each certificate.
                for certificate in certificates {
                    sender
                        .send_confirmed_certificate(&certificate, delivery)
                        .await?;
                }

                Ok::<_, chain_client::Error>(())
            }
        }))
        .await?;
        Ok(())
    }

    /// Logs a warning if the error is not an expected part of the protocol flow.
    fn warn_if_unexpected(&self, err: &NodeError) {
        if !err.is_expected() {
            tracing::warn!(
                remote_node = self.remote_node.address(),
                %err,
                "unexpected error from validator",
            );
        }
    }
}
