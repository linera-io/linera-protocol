// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashSet;

use linera_base::identifiers::{BlobId, BlobType};
use linera_chain::types::{CertificateValue, ConfirmedBlock};
use linera_execution::{system::AdminOperation, Operation, SystemOperation};
use linera_storage::Storage;

use crate::{
    common::{BlockId, CanonicalBlock},
    storage::BlockProcessorStorage,
    ExporterError,
};

pub(super) struct Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    path: Vec<NodeVisitor>,
    visited: HashSet<BlockId>,
    new_committee_blob: Option<BlobId>,
    storage: &'a mut BlockProcessorStorage<S>,
}

impl<'a, S> Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(super) fn new(storage: &'a mut BlockProcessorStorage<S>) -> Self {
        Self {
            storage,
            path: Vec::new(),
            visited: HashSet::new(),
            new_committee_blob: None,
        }
    }

    /// Walks through the block's dependencies in a depth wise manner
    /// resolving, sorting and indexing all of them along the way.
    pub(super) async fn walk(mut self, block: BlockId) -> Result<Option<BlobId>, ExporterError> {
        if self.is_block_indexed(&block).await? {
            return Ok(None);
        }

        let node_visitor = self.get_processed_block_node(&block).await?;
        self.path.push(node_visitor);
        while let Some(mut node_visitor) = self.path.pop() {
            if self.visited.contains(&node_visitor.node.block) {
                continue;
            }

            // resolve block dependencies
            if let Some(dependency) = node_visitor.next_dependency() {
                self.path.push(node_visitor);
                if !self.is_block_indexed(&dependency).await? {
                    let dependency_node = self.get_processed_block_node(&dependency).await?;
                    self.path.push(dependency_node);
                }

                continue;
            }

            // all the block dependecies have been resolved for this block
            // now just resolve the blobs
            let mut blobs_to_send = Vec::new();
            let mut blobs_to_index_block_with = Vec::new();
            for id in node_visitor.node.required_blobs {
                if !self.is_blob_indexed(id).await? {
                    blobs_to_index_block_with.push(id);
                    if !node_visitor.node.created_blobs.contains(&id) {
                        blobs_to_send.push(id);
                    }
                }
            }

            let block_id = node_visitor.node.block;
            if self.index_block(&block_id).await? {
                let block_to_push = CanonicalBlock::new(block_id.hash, &blobs_to_send);
                self.storage.push_block(block_to_push);
                for blob in blobs_to_index_block_with {
                    let _ = self.storage.index_blob(blob);
                }
            }

            self.new_committee_blob = node_visitor.node.new_committee_blob;
            self.visited.insert(block_id);
        }

        Ok(self.new_committee_blob)
    }

    async fn get_processed_block_node(
        &self,
        block_id: &BlockId,
    ) -> Result<NodeVisitor, ExporterError> {
        let block = self.storage.get_block(block_id.hash).await?;
        let processed_block = ProcessedBlock::process_block(block.value());
        let node = NodeVisitor::new(processed_block);
        Ok(node)
    }

    async fn is_block_indexed(&mut self, block_id: &BlockId) -> Result<bool, ExporterError> {
        match self.storage.is_block_indexed(block_id).await {
            Ok(ok) => Ok(ok),
            Err(ExporterError::UnprocessedChain) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn index_block(&mut self, block_id: &BlockId) -> Result<bool, ExporterError> {
        self.storage.index_block(block_id).await
    }

    async fn is_blob_indexed(&mut self, blob_id: BlobId) -> Result<bool, ExporterError> {
        self.storage.is_blob_indexed(blob_id).await
    }
}

struct NodeVisitor {
    node: ProcessedBlock,
    next_dependency: usize,
}

impl NodeVisitor {
    fn new(processed_block: ProcessedBlock) -> Self {
        Self {
            next_dependency: 0,
            node: processed_block,
        }
    }

    fn next_dependency(&mut self) -> Option<BlockId> {
        if let Some(block_id) = self.node.dependencies.get(self.next_dependency) {
            self.next_dependency += 1;
            return Some(*block_id);
        }

        None
    }
}

#[derive(Debug)]
struct ProcessedBlock {
    block: BlockId,
    // blobs created by this block
    // used for filtering which blobs
    // we won't need to send separately
    // as these blobs are part of the block itself.
    created_blobs: Vec<BlobId>,
    // all the blobs required by this block
    required_blobs: Vec<BlobId>,
    dependencies: Vec<BlockId>,
    new_committee_blob: Option<BlobId>,
}

impl ProcessedBlock {
    fn process_block(block: &ConfirmedBlock) -> Self {
        let block_id = BlockId::new(block.chain_id(), block.hash(), block.height());
        let mut dependencies = Vec::new();
        if let Some(parent_hash) = block.block().header.previous_block_hash {
            let height = block_id
                .height
                .try_sub_one()
                .expect("parent only exists if child's height is greater than zero");
            let parent = BlockId::new(block_id.chain_id, parent_hash, height);
            dependencies.push(parent);
        }

        let message_senders = block
            .block()
            .body
            .incoming_bundles()
            .map(BlockId::from_incoming_bundle);
        dependencies.extend(message_senders);

        let new_committee = block.block().body.operations().find_map(|m| {
            if let Operation::System(boxed) = m {
                if let SystemOperation::Admin(AdminOperation::CreateCommittee {
                    blob_hash, ..
                }) = boxed.as_ref()
                {
                    let committee_blob = BlobId::new(*blob_hash, BlobType::Committee);
                    return Some(committee_blob);
                }
            }

            None
        });

        Self {
            dependencies,
            block: block_id,
            new_committee_blob: new_committee,
            required_blobs: block.required_blob_ids().into_iter().collect(),
            created_blobs: block.block().created_blob_ids().into_iter().collect(),
        }
    }
}
