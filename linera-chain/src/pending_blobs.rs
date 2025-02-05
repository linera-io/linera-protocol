// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use async_graphql::SimpleObject;
use linera_base::{
    data_types::{Blob, Round},
    ensure,
    identifiers::BlobId,
};
use linera_views::{
    context::Context,
    map_view::MapView,
    register_view::RegisterView,
    views::{ClonableView, View, ViewError},
};

use crate::ChainError;

/// The pending blobs belonging to a block that can't be processed without them.
#[derive(Debug, View, ClonableView, SimpleObject)]
pub struct PendingBlobsView<C>
where
    C: Clone + Context + Send + Sync + 'static,
{
    /// The round in which the block is validated.
    pub round: RegisterView<C, Round>,
    /// Whether these blobs were already validated.
    ///
    /// This is only `false` for _new_ block proposals, not when re-proposing blocks from earlier
    /// rounds or when handling validated block certificates. If it is false, the pending blobs are
    /// only the ones published by the new block, not the ones that are only read.
    pub validated: RegisterView<C, bool>,
    /// The map of blobs needed to process the block.
    pub pending_blobs: MapView<C, BlobId, Option<Blob>>,
}

impl<C> PendingBlobsView<C>
where
    C: Clone + Context + Send + Sync + 'static,
{
    pub async fn get(&self, blob_id: &BlobId) -> Result<Option<Blob>, ViewError> {
        Ok(self.pending_blobs.get(blob_id).await?.flatten())
    }

    /// Inserts the blob. Returns whether the blob was required by the pending block.
    pub async fn maybe_insert(&mut self, blob: &Blob) -> Result<bool, ViewError> {
        let blob_id = blob.id();
        let Some(maybe_blob) = self.pending_blobs.get_mut(&blob_id).await? else {
            return Ok(false);
        };
        if maybe_blob.is_none() {
            *maybe_blob = Some(blob.clone());
        }
        Ok(true)
    }

    pub async fn update(
        &mut self,
        round: Round,
        validated: bool,
        maybe_blobs: BTreeMap<BlobId, Option<Blob>>,
    ) -> Result<(), ChainError> {
        let existing_round = *self.round.get();
        ensure!(
            existing_round <= round,
            ChainError::InsufficientRound(existing_round)
        );
        if existing_round < round {
            self.clear();
            self.round.set(round);
            self.validated.set(validated);
        }
        for (blob_id, maybe_blob) in maybe_blobs {
            self.pending_blobs.insert(&blob_id, maybe_blob)?;
        }
        Ok(())
    }
}
