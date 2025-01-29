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

    pub async fn maybe_insert(&mut self, blob: &Blob) -> Result<(), ViewError> {
        let blob_id = blob.id();
        if let Some(maybe_blob) = self.pending_blobs.get_mut(&blob_id).await? {
            if maybe_blob.is_none() {
                *maybe_blob = Some(blob.clone());
            }
        }
        Ok(())
    }

    pub async fn update(
        &mut self,
        round: Round,
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
        }
        for (blob_id, maybe_blob) in maybe_blobs {
            self.pending_blobs.insert(&blob_id, maybe_blob)?;
        }
        Ok(())
    }
}
