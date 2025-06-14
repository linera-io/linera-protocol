// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, time::Duration};

use linera_storage::Storage;
use tokio::{
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    time::{interval, MissedTickBehavior},
};

use crate::{
    common::{BlockId, ExporterError},
    runloops::block_processor::walker::Walker,
    storage::BlockProcessorStorage,
};

mod walker;

pub(super) struct BlockProcessor<T>
where
    T: Storage + Clone + Send + Sync + 'static,
{
    storage: BlockProcessorStorage<T>,
    queue_rear: UnboundedSender<BlockId>,
    queue_front: UnboundedReceiver<BlockId>,
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
        shutdown_signal: F,
        persistence_period: u32,
    ) -> Result<(), ExporterError>
    where
        F: IntoFuture<Output = ()>,
    {
        let shutdown_signal_future = shutdown_signal.into_future();
        let mut pinned_shutdown_signal = Box::pin(shutdown_signal_future);

        let mut interval = interval(Duration::from_millis(persistence_period.into()));
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            tokio::select! {

                biased;

                _ = &mut pinned_shutdown_signal => break,

                _ = interval.tick() => self.storage.save().await?,

                Some(next_block_notification) = self.queue_front.recv() => {
                    let walker = Walker::new(&mut self.storage);
                    // this error variant is safe to retry as this block is already confirmed so this error will
                    // orignate from things like missing dependencies or io error.
                    // Other error variants are either safe to skip or unreachable.
                    if let Err(ExporterError::ViewError(_)) = walker.walk(next_block_notification).await {
                        // return the block to the back of the task queue to process again later
                        let _ = self.queue_rear.send(next_block_notification);
                    }
                },

            }
        }

        self.storage.save().await?;

        Ok(())
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

        for (i, (x, y)) in expected_state.into_iter().enumerate() {
            let hash = exporter_storage.get_block(i).await?.hash();
            assert_eq!(hash, state[x][y]);
        }

        Ok(())
    }

    // A scenario to test topological sort with
    // populates the storage with two chains, each with height of four blocks.
    // Blocks have a dependency with the blocks of the chains that came before
    // chronologically during creation
    async fn make_state<S: Storage>(storage: &S) -> (Vec<BlockId>, Vec<Vec<CryptoHash>>) {
        let mut notifications = Vec::new();

        let chain_id_a = ChainId(CryptoHash::test_hash("0"));
        let chain_id_b = ChainId(CryptoHash::test_hash("1"));

        let mut chain_a = Vec::new();
        let mut chain_b = Vec::new();

        for i in 0..4 {
            if i == 0 {
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

        for (i, (x, y)) in expected_state.into_iter().enumerate() {
            let hash = exporter_storage.get_block(i).await?.hash();
            assert_eq!(hash, state[x][y]);
        }

        Ok(())
    }

    // A scenario to test topological sort with
    // populates the storage with three chains each with height of two blocks.
    // Blocks have a dependency with the blocks of the chains that came before
    // chronologically during creation
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
