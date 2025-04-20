// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, VecDeque};

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob as ConfirmedBlob, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::types::ConfirmedBlock;
use linera_client::config::DestinationId;
use linera_sdk::views::{RootView, View, ViewError};
use linera_storage::Storage;
use linera_views::{
    context::Context, map_view::MapView, reentrant_collection_view::ReentrantCollectionView,
    register_view::RegisterView, views::ClonableView,
};
use serde::{Deserialize, Serialize};

#[cfg(with_testing)]
use crate::exporter_service::Summary;
use crate::ExporterError;

/// State of the linera exporter as a view.
#[derive(Debug, RootView, ClonableView)]
pub(super) struct BlockExporterStateView<C> {
    /// The chain status, by chain ID.
    state: ReentrantCollectionView<C, ChainId, ChainStatusView<C>>,
    /// The block(and blob) index.
    index: MapView<C, Key, Value>,
}

#[derive(Debug, Clone, Serialize, Ord, PartialOrd, Eq, PartialEq, Deserialize, Hash)]
pub(super) enum Key {
    Block(CryptoHash),
    Blob(BlobId),
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(super) struct Block {
    pub(super) dependencies: DependencySet,
    pub(super) destinatons: BTreeSet<DestinationId>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(super) struct Blob {
    pub(super) destinatons: BTreeSet<DestinationId>,
}

pub(super) type DependencySet = BTreeSet<Key>;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(super) enum Value {
    Block(Block),
    Blob(Blob),
}

impl Block {
    fn new(set: DependencySet) -> Block {
        Block {
            dependencies: set,
            destinatons: BTreeSet::new(),
        }
    }
}

impl Blob {
    fn new() -> Blob {
        Blob {
            destinatons: BTreeSet::new(),
        }
    }
}

impl Value {
    fn new_block(set: DependencySet) -> Value {
        Block::new(set).into()
    }

    fn new_blob() -> Value {
        Blob::new().into()
    }
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
        let height = self
            .next_heights_to_process
            .get_mut_or_default(&destination)
            .await?;
        *height = height.try_add_one().map_err(ViewError::ArithmeticError)?;
        Ok(())
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
        (height, hash): (BlockHeight, CryptoHash),
    ) -> Result<(), ExporterError> {
        let mut guard = self.state.try_load_entry_mut(chain_id).await?;
        guard.update_block_height(height, hash);
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

    pub fn index_block(&mut self, hash: CryptoHash, set: DependencySet) -> Result<(), ViewError> {
        let key = Key::Block(hash);
        let value = Value::new_block(set);
        self.index.insert(&key, value)
    }

    pub fn index_blob(&mut self, blob: BlobId) -> Result<(), ViewError> {
        let key = Key::Blob(blob);
        let value = Value::new_blob();
        self.index.insert(&key, value)
    }

    pub async fn get_block(&mut self, hash: CryptoHash) -> Result<Option<&mut Block>, ViewError> {
        let result = self.index.get_mut(&hash.into()).await?;
        match result {
            None => Ok(None),
            Some(Value::Block(block)) => Ok(Some(block)),
            _ => Err(ViewError::InconsistentEntries),
        }
    }

    pub async fn get_blob(&mut self, blob: BlobId) -> Result<Option<&mut Blob>, ViewError> {
        let result = self.index.get_mut(&blob.into()).await?;
        match result {
            None => Ok(None),
            Some(Value::Blob(blob)) => Ok(Some(blob)),
            _ => Err(ViewError::InconsistentEntries),
        }
    }

    pub async fn contains_block(&self, hash: CryptoHash) -> Result<bool, ViewError> {
        match self.index.get(&hash.into()).await? {
            None => Ok(false),
            Some(Value::Block(_)) => Ok(true),
            _ => Err(ViewError::InconsistentEntries),
        }
    }

    pub async fn contains_blob(&self, blob: BlobId) -> Result<bool, ViewError> {
        match self.index.get(&blob.into()).await? {
            None => Ok(false),
            Some(Value::Blob(_)) => Ok(true),
            _ => Err(ViewError::InconsistentEntries),
        }
    }
}

impl From<Block> for Value {
    fn from(value: Block) -> Self {
        Self::Block(value)
    }
}

impl From<Blob> for Value {
    fn from(value: Blob) -> Self {
        Self::Blob(value)
    }
}

impl From<CryptoHash> for Key {
    fn from(value: CryptoHash) -> Self {
        Key::Block(value)
    }
}

impl From<BlobId> for Key {
    fn from(value: BlobId) -> Self {
        Key::Blob(value)
    }
}

pub(super) struct Batch {
    block: ConfirmedBlock,
    block_dependencies: Vec<ConfirmedBlock>,
    blob_dependencies: Vec<ConfirmedBlob>,
}

impl Batch {
    pub(super) fn new(block: ConfirmedBlock) -> Batch {
        Self {
            block,
            block_dependencies: Vec::new(),
            blob_dependencies: Vec::new(),
        }
    }

    pub(super) fn add_block(&mut self, block: ConfirmedBlock) {
        self.block_dependencies.push(block);
    }

    pub(super) fn add_blob(&mut self, blob: ConfirmedBlob) {
        self.blob_dependencies.push(blob);
    }

    #[cfg(with_testing)]
    pub(super) fn into_summary(self) -> Summary {
        let mut summary = Summary::new(&self.block);

        for block in self.block_dependencies {
            summary.add_block(&block);
        }

        for blob in self.blob_dependencies {
            summary.add_blob(&blob);
        }

        summary
    }
}

#[async_trait]
pub(super) trait StorageExt {
    type Key;
    type Batch;
    type Error;

    async fn read_batch(&self, batch: Vec<Self::Key>) -> Result<Self::Batch, Self::Error>;
}

#[async_trait]
impl<S> StorageExt for S
where
    S: Storage + Clone + Send + Sync + 'static,
{
    type Key = Key;
    type Batch = Batch;
    type Error = ExporterError;

    async fn read_batch(&self, keys: Vec<Self::Key>) -> Result<Self::Batch, Self::Error> {
        let mut keys = VecDeque::from(keys);
        let identity_block_key = keys.pop_front().ok_or(ExporterError::GenericError(
            "batch should contain the identity block".into(),
        ))?;
        let Key::Block(hash) = identity_block_key else {
            return Err(ExporterError::GenericError(
                "batch should contain the identity block".into(),
            ));
        };

        let identity_block = self.read_confirmed_block(hash).await?;
        let mut batch = Batch::new(identity_block);
        for key in keys {
            match key {
                Key::Block(hash) => {
                    let block = self.read_confirmed_block(hash).await?;
                    batch.add_block(block);
                }
                Key::Blob(blob_id) => {
                    let blob = self.read_blob(blob_id).await?;
                    batch.add_blob(blob);
                }
            }
        }

        Ok(batch)
    }
}
