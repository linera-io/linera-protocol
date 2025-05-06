// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, future::IntoFuture, sync::Arc, time::Duration};

use linera_base::crypto::CryptoHash;
use linera_chain::types::{Block, ConfirmedBlockCertificate};
use linera_storage::Storage;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::{interval, MissedTickBehavior},
};

use crate::{
    common::{BlockId, ExporterError, LiteBlockId},
    storage::BlockProcessorStorage,
};

pub(super) struct BlockProcessor<T>
where
    T: Storage + Clone + Send + Sync + 'static,
{
    storage: BlockProcessorStorage<T>,
    queue_rear: UnboundedSender<BlockId>,
    queue_front: UnboundedReceiver<BlockId>,
}

struct Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    path: Vec<NodeVisitor>,
    visited: HashSet<BlockId>,
    storage: &'a mut BlockProcessorStorage<S>,
}

#[derive(Debug)]
struct ProcessedBlock {
    block: BlockId,
    siblings: Vec<BlockId>,
    parent: Option<LiteBlockId>,
}

struct NodeVisitor {
    node: ProcessedBlock,
    sibling_branch: usize,
}

impl<T> BlockProcessor<T>
where
    T: Storage + Clone + Send + Sync + 'static,
{
    pub(super) fn new(
        storage: BlockProcessorStorage<T>,
        queue_rear: UnboundedSender<BlockId>,
        queue_front: UnboundedReceiver<BlockId>,
    ) -> Self {
        Self {
            storage,
            queue_rear,
            queue_front,
        }
    }

    pub(super) async fn run_with_shutdown<F>(
        &mut self,
        signal: F,
        persistence_period: u16,
    ) -> Result<(), ExporterError>
    where
        F: IntoFuture<Output = ()>,
    {
        let furure = signal.into_future();
        let mut pinned = Box::pin(furure);

        let mut interval = interval(Duration::from_secs(persistence_period.into()));
        interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

        loop {
            tokio::select! {

                biased;

                _ = &mut pinned => break,

                _ = interval.tick() => self.storage.save().await?,

                Some(next_block_notification) = self.queue_front.recv() => {
                    let walker = Walker::new(&mut self.storage);
                    if let Err(_err) = walker.walk(next_block_notification.clone()).await {
                        // return the block to the back of the task queue to process again later
                        let _ = self.queue_rear.send(next_block_notification);
                    }
                },

            }
        }

        Ok(())
    }
}

impl<'a, S> Walker<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(storage: &'a mut BlockProcessorStorage<S>) -> Self {
        Self {
            path: Vec::new(),
            visited: HashSet::new(),
            storage,
        }
    }

    async fn walk(mut self, block: BlockId) -> Result<(), ExporterError> {
        if self.is_block_indexed(&block).await? {
            return Ok(());
        }

        let node_visitor = self.get_processed_block_node(&block).await?;
        self.path.push(node_visitor);
        while let Some(mut node_visitor) = self.path.pop() {
            if self.visited.contains(&node_visitor.node.block) {
                continue;
            }

            // resolve ancestors
            let parent = node_visitor.get_parent();
            if let Some(parent) = parent {
                if !self.is_block_indexed(&parent).await? {
                    let parent_node = self.get_processed_block_node(&parent).await?;
                    self.path.push(node_visitor);
                    self.path.push(parent_node);
                    continue;
                }
            }

            // resolve siblings
            let sibling = node_visitor.node.siblings.get(node_visitor.sibling_branch);
            if let Some(sibling) = sibling {
                node_visitor.sibling_branch += 1;
                if !self.is_block_indexed(sibling).await? {
                    let sibling_node = self.get_processed_block_node(sibling).await?;
                    self.path.push(node_visitor);
                    self.path.push(sibling_node);
                    continue;
                }
            }

            let block_id = node_visitor.node.block.clone();
            self.visited.insert(block_id.clone());
            self.index_block(&block_id).await?;
        }

        Ok(())
    }

    async fn get_processed_block_node(
        &self,
        block_id: &BlockId,
    ) -> Result<NodeVisitor, ExporterError> {
        let block = self.get_block(block_id.hash).await?;
        let processed_block = ProcessedBlock::process_block(block_id.clone(), block.block());
        let node = NodeVisitor::new(processed_block);
        Ok(node)
    }

    async fn get_block(
        &self,
        hash: CryptoHash,
    ) -> Result<Arc<ConfirmedBlockCertificate>, ExporterError> {
        self.storage.get_block(hash).await
    }

    async fn is_block_indexed(&mut self, block_id: &BlockId) -> Result<bool, ExporterError> {
        match self.storage.is_block_indexed(block_id).await {
            Ok(ok) => Ok(ok),
            Err(ExporterError::UnprocessedChain) => Ok(false),
            Err(e) => Err(e),
        }
    }

    async fn index_block(&mut self, block_id: &BlockId) -> Result<(), ExporterError> {
        self.storage.index_block(block_id).await.unwrap();
        Ok(())
    }
}

impl NodeVisitor {
    fn new(processed_block: ProcessedBlock) -> Self {
        Self {
            node: processed_block,
            sibling_branch: 0,
        }
    }

    fn get_parent(&self) -> Option<BlockId> {
        self.node
            .parent
            .clone()
            .map(|lite| lite.with_chain_id(self.node.block.chain_id))
    }
}

impl ProcessedBlock {
    fn process_block(block_id: BlockId, block: &Block) -> Self {
        Self {
            parent: block.header.previous_block_hash.map(|hash| {
                LiteBlockId::new(
                    block_id
                        .height
                        .try_sub_one()
                        .expect("parent only exists if child's height is greater than zero"),
                    hash,
                )
            }),
            block: block_id,
            siblings: block
                .body
                .incoming_bundles
                .iter()
                .map(BlockId::from_incoming_bundle)
                .collect::<Vec<_>>(),
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use linera_base::{
        crypto::CryptoHash,
        data_types::{Round, Timestamp},
        identifiers::ChainId,
    };
    use linera_chain::{
        data_types::{BlockExecutionOutcome, IncomingBundle, MessageBundle},
        test::{make_child_block, make_first_block, BlockTestExt},
        types::{CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_client::config::LimitsConfig;
    use linera_sdk::test::MessageAction;
    use linera_storage::{DbStorage, Storage};
    use linera_views::memory::MemoryStore;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_util::sync::CancellationToken;

    use crate::{
        common::BlockId, runloops::BlockProcessor, storage::BlockProcessorStorage,
        ExporterCancellationSignal,
    };

    #[tokio::test]
    async fn test_topological_sort() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let (block_processor_storage, exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, 0, LimitsConfig::default()).await?;
        let mut block_processor = BlockProcessor::new(block_processor_storage, tx.clone(), rx);
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let (block_ids, state) = make_state(&storage).await;
        for id in block_ids {
            let _ = tx.send(id);
        }

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = block_processor.run_with_shutdown(signal, 5) => {},
        }

        // oredered pair of (chain_id, block_height)
        let expected_state = [
            (1, 0),
            (0, 0),
            (0, 1),
            (1, 1),
            (1, 2),
            (0, 2),
            (0, 3),
            (1, 3),
        ];

        let mut i = 0;
        for (x, y) in expected_state {
            let hash = exporter_storage.get_block(i).await?.hash();
            assert_eq!(hash, state[x][y]);
            i += 1;
        }

        Ok(())
    }

    async fn make_state<S: Storage>(storage: &S) -> (Vec<BlockId>, Vec<Vec<CryptoHash>>) {
        let mut notifications = Vec::new();

        let chain_id_a = ChainId(CryptoHash::test_hash("0"));
        let chain_id_b = ChainId(CryptoHash::test_hash("1"));

        let mut chain_a = Vec::new();
        let mut chain_b = Vec::new();

        for i in 0..4 {
            if 0 == i {
                let block_a = ConfirmedBlock::new(
                    BlockExecutionOutcome::default().with(make_first_block(chain_id_a)),
                );
                let block_b = ConfirmedBlock::new(
                    BlockExecutionOutcome::default().with(make_first_block(chain_id_b)),
                );
                chain_a.push(block_a);
                chain_b.push(block_b);
                continue;
            }

            let block_a = ConfirmedBlock::new(
                BlockExecutionOutcome::default().with(make_child_block(chain_a.last().unwrap())),
            );
            chain_a.push(block_a);

            let block_b = if i % 2 == 0 {
                ConfirmedBlock::new(
                    BlockExecutionOutcome::default()
                        .with(make_child_block(chain_b.iter().last().unwrap())),
                )
            } else {
                let incoming_bundle = IncomingBundle {
                    origin: chain_id_a,
                    bundle: MessageBundle {
                        height: (i as u64).into(),
                        timestamp: Timestamp::now(),
                        certificate_hash: chain_a.get(i as usize).unwrap().hash(),
                        transaction_index: 0,
                        messages: vec![],
                    },
                    action: MessageAction::Accept,
                };

                let block_b = ConfirmedBlock::new(BlockExecutionOutcome::default().with(
                    make_child_block(chain_b.last().unwrap()).with_incoming_bundle(incoming_bundle),
                ));
                let block_id = get_block_id(&block_b);
                notifications.push(block_id);
                block_b
            };

            chain_b.push(block_b);
        }

        for block in chain_a.iter().chain(chain_b.iter()) {
            let cert = ConfirmedBlockCertificate::new(block.clone(), Round::Fast, vec![]);
            storage
                .write_blobs_and_certificate(&[], &cert)
                .await
                .unwrap();
        }

        (
            notifications,
            vec![
                chain_a.iter().map(|block| block.inner().hash()).collect(),
                chain_b.iter().map(|block| block.inner().hash()).collect(),
            ],
        )
    }

    fn get_block_id(block: &ConfirmedBlock) -> BlockId {
        BlockId::new(block.chain_id(), block.inner().hash(), block.height())
    }

    #[tokio::test]
    async fn test_topological_sort_2() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let (block_processor_storage, exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, 0, LimitsConfig::default()).await?;
        let mut block_processor = BlockProcessor::new(block_processor_storage, tx.clone(), rx);
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let (block_id, state) = make_state_2(&storage).await;
        let _ = tx.send(block_id);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = block_processor.run_with_shutdown(signal, 5) => {},
        }

        let expected_state = [(2, 0), (1, 0), (0, 0), (0, 1), (1, 1), (2, 1)];

        let mut i = 0;
        for (x, y) in expected_state {
            let hash = exporter_storage.get_block(i).await?.hash();
            assert_eq!(hash, state[x][y]);
            i += 1;
        }

        Ok(())
    }

    async fn make_state_2<S: Storage>(storage: &S) -> (BlockId, Vec<Vec<CryptoHash>>) {
        let chain_id_a = ChainId(CryptoHash::test_hash("0"));
        let chain_id_b = ChainId(CryptoHash::test_hash("1"));
        let chain_id_c = ChainId(CryptoHash::test_hash("2"));

        let get_bundle = |sender_block: &ConfirmedBlock| IncomingBundle {
            origin: sender_block.chain_id(),
            bundle: MessageBundle {
                height: sender_block.height(),
                timestamp: Timestamp::now(),
                certificate_hash: sender_block.inner().hash(),
                transaction_index: 0,
                messages: vec![],
            },
            action: MessageAction::Accept,
        };

        let mut state = Vec::new();

        let block_1_a = ConfirmedBlock::new(
            BlockExecutionOutcome::default().with(make_first_block(chain_id_a)),
        );
        let block_2_a = ConfirmedBlock::new(
            BlockExecutionOutcome::default().with(make_child_block(&block_1_a)),
        );

        let block_1_b = ConfirmedBlock::new(
            BlockExecutionOutcome::default().with(make_first_block(chain_id_b)),
        );
        let block_2_b = ConfirmedBlock::new(
            BlockExecutionOutcome::default()
                .with(make_child_block(&block_1_b).with_incoming_bundle(get_bundle(&block_2_a))),
        );

        let block_1_c = ConfirmedBlock::new(
            BlockExecutionOutcome::default().with(make_first_block(chain_id_c)),
        );
        let block_2_c = ConfirmedBlock::new(
            BlockExecutionOutcome::default()
                .with(make_child_block(&block_1_c).with_incoming_bundle(get_bundle(&block_2_b))),
        );

        let notification = get_block_id(&block_2_c);

        state.push(vec![block_1_a, block_2_a]);
        state.push(vec![block_1_b, block_2_b]);
        state.push(vec![block_1_c, block_2_c]);

        for block in state.iter().flatten() {
            let cert = ConfirmedBlockCertificate::new(block.clone(), Round::Fast, vec![]);
            storage
                .write_blobs_and_certificate(&[], &cert)
                .await
                .unwrap()
        }

        (
            notification,
            state
                .iter()
                .map(|chain| chain.iter().map(|block| block.inner().hash()).collect())
                .collect(),
        )
    }
}
