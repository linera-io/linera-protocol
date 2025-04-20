// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::{BTreeSet, HashSet};

use linera_base::{
    crypto::CryptoHash,
    data_types::Blob as ConfirmedBlob,
    identifiers::{ApplicationId, BlobId, BlobType},
};
use linera_chain::{data_types::IncomingBundle, types::ConfirmedBlock};
use linera_client::config::DestinationId;
use linera_execution::{system::AdminOperation, Operation, SystemOperation};
use linera_storage::Storage;
use tokio::sync::MutexGuard;

use crate::{
    state::{Blob, Block, BlockExporterStateView, DependencySet, Key},
    ExporterError,
};

pub(super) struct BlockProcessor<'s, T>
where
    T: Storage + Clone + Send + Sync + 'static,
{
    destination_id: DestinationId,
    walker: Walker<'s, T>,
}

impl<'a, S> BlockProcessor<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub fn new(
        destination_id: DestinationId,
        storage: &'a S,
        state: MutexGuard<'a, BlockExporterStateView<<S as Storage>::BlockExporterContext>>,
    ) -> Self {
        let walker = Walker::new(storage, state);
        Self {
            destination_id,
            walker,
        }
    }

    pub async fn process_block(&mut self, hash: CryptoHash) -> Result<Vec<Key>, ExporterError> {
        let mut batch = Vec::new();
        self.walker.walk(hash.into(), self.destination_id).await?;
        self.walker.visited.remove(&hash.into());
        batch.push(hash.into());
        batch.append(&mut self.walker.consolidate());
        Ok(batch)
    }

    pub fn destructor(
        self,
    ) -> MutexGuard<'a, BlockExporterStateView<<S as Storage>::BlockExporterContext>> {
        self.walker.state
    }
}

struct Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    path: Vec<Key>,
    visited: HashSet<Key>,
    state: MutexGuard<'a, BlockExporterStateView<<S as Storage>::BlockExporterContext>>,
    storage: &'a S,
}

impl<'a, S> Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        storage: &'a S,
        state: MutexGuard<'a, BlockExporterStateView<<S as Storage>::BlockExporterContext>>,
    ) -> Walker<'a, S> {
        Walker {
            path: Vec::new(),
            visited: HashSet::new(),
            storage,
            state,
        }
    }

    fn consolidate(&mut self) -> Vec<Key> {
        self.path = Vec::new();
        std::mem::take(&mut self.visited)
            .into_iter()
            .collect::<Vec<_>>()
    }
}

impl<'a, S> Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn walk(&mut self, key: Key, _destination_id: u16) -> Result<(), ExporterError> {
        let destination_id = 0;
        match key {
            Key::Blob(_) => {}
            Key::Block(hash) => {
                self.path.push(hash.into());
                while let Some(key) = self.path.pop() {
                    match key {
                        Key::Block(hash) => self.visit_block(hash, destination_id).await?,
                        Key::Blob(blob) => self.visit_blob(blob, destination_id).await?,
                    }
                }
            }
        }

        Ok(())
    }

    async fn visit_block(
        &mut self,
        hash: CryptoHash,
        destination_id: u16,
    ) -> Result<(), ExporterError> {
        if self
            .get_indexed_block(hash)
            .await?
            .destinatons
            .insert(destination_id)
        {
            let dependencies = self.get_indexed_block(hash).await?.dependencies.clone();
            self.path.extend(dependencies);
            self.visited.insert(hash.into());
        }

        Ok(())
    }

    async fn visit_blob(&mut self, blob: BlobId, destination_id: u16) -> Result<(), ExporterError> {
        if self
            .get_indexed_blob(blob)
            .await?
            .destinatons
            .insert(destination_id)
        {
            self.visited.insert(blob.into());
        }

        Ok(())
    }

    async fn get_indexed_block(&mut self, hash: CryptoHash) -> Result<&mut Block, ExporterError> {
        if !self.state.contains_block(hash).await? {
            let block = self.download_block(hash).await?;
            let set = self.process_block(block).await?;
            self.state.index_block(hash, set)?;
        }

        Ok(self.state.get_block(hash).await?.unwrap())
    }

    async fn get_indexed_blob(&mut self, blob: BlobId) -> Result<&mut Blob, ExporterError> {
        if !self.state.contains_blob(blob).await? {
            self.state.index_blob(blob)?;
        }

        Ok(self.state.get_blob(blob).await?.unwrap())
    }

    async fn download_block(&self, hash: CryptoHash) -> Result<ConfirmedBlock, ExporterError> {
        let block = self.storage.read_confirmed_block(hash).await?;
        Ok(block)
    }

    async fn process_block(&self, block: ConfirmedBlock) -> Result<DependencySet, ExporterError> {
        let block = block.into_block();
        let mut dependencies: BTreeSet<Key> = BTreeSet::new();

        for bundle in &block.body.incoming_bundles {
            let bundle: &IncomingBundle = bundle;
            let sender_block_hash = bundle.bundle.certificate_hash;
            dependencies.insert(sender_block_hash.into());
        }

        for operation in &block.body.operations {
            let operation: &Operation = operation;
            match operation {
                Operation::User {
                    application_id,
                    bytes: _,
                } => {
                    dependencies.insert(application_id.description_blob_id().into());
                }

                Operation::System(system_operation) => match system_operation.as_ref() {
                    SystemOperation::PublishModule { module_id } => {
                        dependencies.insert(module_id.contract_bytecode_blob_id().into());
                        dependencies.insert(module_id.service_bytecode_blob_id().into());
                    }

                    SystemOperation::PublishDataBlob { blob_hash } => {
                        dependencies.insert(BlobId::new(*blob_hash, BlobType::Data).into());
                    }

                    SystemOperation::ReadBlob { blob_id } => {
                        dependencies.insert((*blob_id).into());
                    }

                    SystemOperation::CreateApplication {
                        module_id,
                        parameters: _,
                        instantiation_argument: _,
                        required_application_ids,
                    } => {
                        dependencies.insert(module_id.contract_bytecode_blob_id().into());
                        dependencies.insert(module_id.service_bytecode_blob_id().into());

                        for required_application_id in required_application_ids {
                            let required_application_id: &ApplicationId = required_application_id;
                            dependencies
                                .insert(required_application_id.description_blob_id().into());
                        }
                    }

                    SystemOperation::Admin(admin_operation) => match admin_operation {
                        AdminOperation::PublishCommitteeBlob { blob_hash } => {
                            dependencies
                                .insert(BlobId::new(*blob_hash, BlobType::Committee).into());
                        }

                        AdminOperation::CreateCommittee {
                            epoch: _,
                            blob_hash,
                        } => {
                            dependencies
                                .insert(BlobId::new(*blob_hash, BlobType::Committee).into());
                        }

                        _ => {}
                    },

                    _ => {}
                },
            }
        }

        for blob in block.body.blobs.iter().flatten() {
            let blob: &ConfirmedBlob = blob;
            dependencies.insert(blob.id().into());
        }

        Ok(dependencies)
    }
}
