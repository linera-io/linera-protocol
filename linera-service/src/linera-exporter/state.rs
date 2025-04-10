// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use linera_base::{crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId};
use linera_client::config::DestinationId;
use linera_sdk::views::{RootView, View, ViewError};
use linera_views::{
    context::Context, map_view::MapView, reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView, views::ClonableView,
};

use crate::ExporterError;

/// State of the linera exporter as a view.
#[derive(Debug, RootView, ClonableView)]
pub struct BlockExporterStateView<C> {
    /// The inner state.
    state: ReentrantCollectionView<C, ChainId, ChainStatusView<C>>,
}

/// State of the linera exporter for a particular chain as a view.
#[derive(Debug, View, ClonableView)]
struct ChainStatusView<C> {
    /// Tracks the highest block known to be in storage with its hash.
    /// Hash is needed to retrieve the blocks themselves as a block is stored
    /// in the shared database by its hash as the key.
    known_height: RegisterView<C, Option<(BlockHeight, CryptoHash)>>,
    /// Tracks the highest block already processed (plus one)
    /// for every destination.
    next_heights_to_process: MapView<C, DestinationId, BlockHeight>,
}

impl<C> ChainStatusView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    // Update with the latest [`BlockHeight`] rather than incrementing by one.
    // As in cases notfications are lost, exporter is lagging behind, crashes etc.
    pub fn update_block_height(&mut self, height: BlockHeight, hash: CryptoHash) {
        self.known_height.set(Some((height, hash)));
    }

    pub async fn increment_destination_height(
        &mut self,
        destination: DestinationId,
    ) -> Result<(), ExporterError> {
        match self.next_heights_to_process.get_mut(&destination).await? {
            Some(mutable) => {
                mutable.try_add_one().map_err(ViewError::ArithmeticError)?;
                Ok(())
            }
            None => self.insert_destination(destination),
        }
    }

    pub fn insert_destination(&mut self, destination: DestinationId) -> Result<(), ExporterError> {
        self.next_heights_to_process
            .insert(&destination, 1.into())?;
        Ok(())
    }
}

impl<C> BlockExporterStateView<C>
where
    C: Context + Clone + Send + Sync + 'static,
{
    pub async fn initialize_chain(
        &mut self,
        chain_id: &ChainId,
        height: (BlockHeight, CryptoHash),
    ) -> Result<(), ExporterError> {
        let mut guard = self.state.try_load_entry_mut(chain_id).await?;
        guard.update_block_height(height.0, height.1);
        Ok(())
    }

    pub async fn get_chain_tip(
        &self,
        chain_id: &ChainId,
    ) -> Result<Option<BlockHeight>, ExporterError> {
        let some = self.state.try_load_entry(chain_id).await?;
        let tip = some.and_then(|guard| guard.known_height.get().map(|x| x.0));
        Ok(tip)
    }
}
