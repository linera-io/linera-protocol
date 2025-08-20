// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::{Future, IntoFuture},
    time::Duration,
};

use linera_execution::committee::Committee;
use linera_service::config::DestinationId;
use linera_storage::Storage;
use tokio::time::{interval, MissedTickBehavior};

use crate::{
    common::ExporterError,
    runloops::{block_processor::walker::Walker, ExportersTracker, NewBlockQueue},
    storage::BlockProcessorStorage,
};

mod walker;

pub(super) struct BlockProcessor<F, T>
where
    T: Storage + Clone + Send + Sync + 'static,
{
    exporters_tracker: ExportersTracker<F, T>,
    storage: BlockProcessorStorage<T>,
    new_block_queue: NewBlockQueue,
    committee_destination_update: bool,
}

impl<S, T> BlockProcessor<S, T>
where
    T: Storage + Clone + Send + Sync + 'static,
    S: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <S as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    pub(super) fn new(
        exporters_tracker: ExportersTracker<S, T>,
        storage: BlockProcessorStorage<T>,
        new_block_queue: NewBlockQueue,
        committee_destination_update: bool,
    ) -> Self {
        Self {
            storage,
            exporters_tracker,
            committee_destination_update,
            new_block_queue,
        }
    }

    pub(super) fn pool_state(self) -> ExportersTracker<S, T> {
        self.exporters_tracker
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

        self.exporters_tracker.start_startup_exporters();

        loop {
            tokio::select! {

                biased;

                _ = &mut pinned_shutdown_signal => break,

                _ = interval.tick() => self.storage.save().await?,

                Some(next_block_notification) = self.new_block_queue.recv() => {
                    let walker = Walker::new(&mut self.storage);
                    match walker.walk(next_block_notification).await {
                        Ok(maybe_new_committee) if self.committee_destination_update => {
                            tracing::trace!("new committee blob found, updating the committee destination.");
                            if let Some(blob_id) = maybe_new_committee {
                                let blob = match self.storage.get_blob(blob_id).await {
                                    Ok(blob) => blob,
                                    Err(error) => {
                                        tracing::error!("unable to get the committee blob: {:?} from storage, , received error: {:?}", blob_id, error);
                                        return Err(error);
                                    },
                                };

                                let committee: Committee = match bcs::from_bytes(blob.bytes()) {
                                    Ok(committee) => committee,
                                    Err(e) => {
                                        tracing::error!("unable to serialize the committee blob: {:?}, received error: {:?}", blob_id, e);
                                        continue;
                                    }
                                };

                                let committee_destinations = committee.validator_addresses().map(|(_, address)| DestinationId::validator(address.to_owned())).collect::<Vec<_>>();
                                self.exporters_tracker.shutdown_old_committee(committee_destinations.clone()).await;
                                self.storage.new_committee(committee_destinations.clone());
                                self.exporters_tracker.start_committee_exporters(committee_destinations.clone());
                            }
                        },

                        Ok(_) => {
                            tracing::info!(block=?next_block_notification, "New committee blob found but exporter is not configured \
                             to update the committee destination, skipping.");
                        },

                        // this error variant is safe to retry as this block is already confirmed so this error will
                        // originate from things like missing dependencies or io error.
                        // Other error variants are either safe to skip or unreachable.
                        Err(ExporterError::ViewError(_)) => {
                            // return the block to the back of the task queue to process again later
                            self.new_block_queue.push_back(next_block_notification);
                        },

                        Err(e @ (ExporterError::UnprocessedChain
                                | ExporterError::BadInitialization
                                | ExporterError::ChainAlreadyExists(_))
                            ) => {
                            tracing::error!("error {:?} when resolving block with hash: {}", e, next_block_notification.hash)
                        },

                        Err(e) => {
                            tracing::error!("unexpected error: {:?}", e);
                            return Err(e);
                        }
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
    use linera_base::{
        crypto::CryptoHash,
        data_types::{Round, Timestamp},
        identifiers::ChainId,
        time::Duration,
    };
    use linera_chain::{
        data_types::{BlockExecutionOutcome, IncomingBundle, MessageBundle},
        test::{make_child_block, make_first_block, BlockTestExt},
        types::{CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_rpc::NodeOptions;
    use linera_sdk::test::MessageAction;
    use linera_service::config::LimitsConfig;
    use linera_storage::{DbStorage, Storage, TestClock};
    use linera_views::memory::MemoryDatabase;
    use tokio::sync::mpsc::unbounded_channel;
    use tokio_util::sync::CancellationToken;

    use crate::{
        common::BlockId,
        runloops::{BlockProcessor, ExportersTracker, NewBlockQueue},
        storage::BlockProcessorStorage,
        test_utils::make_simple_state_with_blobs,
        ExporterCancellationSignal,
    };

    #[test_log::test(tokio::test)]
    async fn test_topological_sort() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let new_block_queue = NewBlockQueue {
            queue_rear: tx.clone(),
            queue_front: rx,
        };
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let (block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                .await?;
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let exporters_tracker = ExportersTracker::<
            ExporterCancellationSignal,
            DbStorage<MemoryDatabase, TestClock>,
        >::new(
            NodeOptions::default(),
            0,
            signal.clone(),
            exporter_storage.clone(),
            vec![],
        );
        let mut block_processor = BlockProcessor::new(
            exporters_tracker,
            block_processor_storage,
            new_block_queue,
            false,
        );
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
            let hash = exporter_storage.get_block_with_blob_ids(i).await?.0.hash();
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
                let block_id = BlockId::from_confirmed_block(&block_b);
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

    #[test_log::test(tokio::test)]
    async fn test_topological_sort_2() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let new_block_queue = NewBlockQueue {
            queue_rear: tx.clone(),
            queue_front: rx,
        };
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let (block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                .await?;
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let exporters_tracker = ExportersTracker::<
            ExporterCancellationSignal,
            DbStorage<MemoryDatabase, TestClock>,
        >::new(
            NodeOptions::default(),
            0,
            signal.clone(),
            exporter_storage.clone(),
            vec![],
        );
        let mut block_processor = BlockProcessor::new(
            exporters_tracker,
            block_processor_storage,
            new_block_queue,
            false,
        );
        let (block_id, state) = make_state_2(&storage).await;
        let _ = tx.send(block_id);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = block_processor.run_with_shutdown(signal, 5) => {},
        }

        let expected_state = [(2, 0), (1, 0), (0, 0), (0, 1), (1, 1), (2, 1)];

        for (i, (x, y)) in expected_state.into_iter().enumerate() {
            let hash = exporter_storage.get_block_with_blob_ids(i).await?.0.hash();
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

        let notification = BlockId::from_confirmed_block(&block_2_c);

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

    #[tokio::test]
    async fn test_topological_sort_3() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let new_block_queue = NewBlockQueue {
            queue_rear: tx.clone(),
            queue_front: rx,
        };
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let (block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                .await?;
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let exporters_tracker = ExportersTracker::<
            ExporterCancellationSignal,
            DbStorage<MemoryDatabase, TestClock>,
        >::new(
            NodeOptions::default(),
            0,
            signal.clone(),
            exporter_storage.clone(),
            vec![],
        );
        let mut block_processor = BlockProcessor::new(
            exporters_tracker,
            block_processor_storage,
            new_block_queue,
            false,
        );
        let (block_id, state) = make_state_3(&storage).await;
        let _ = tx.send(block_id);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = block_processor.run_with_shutdown(signal, 5) => {},
        }

        for (index, expected_hash) in state.iter().enumerate() {
            let sorted_hash = exporter_storage
                .get_block_with_blob_ids(index)
                .await?
                .0
                .hash();
            assert_eq!(*expected_hash, sorted_hash);
        }

        Ok(())
    }

    // a simple single chain scenario with four blocks
    async fn make_state_3<S: Storage>(storage: &S) -> (BlockId, Vec<CryptoHash>) {
        let chain_id = ChainId(CryptoHash::test_hash("0"));

        let mut chain = Vec::new();

        for i in 0..4 {
            if i == 0 {
                let block = ConfirmedBlock::new(
                    BlockExecutionOutcome::default().with(make_first_block(chain_id)),
                );
                chain.push(block);
                continue;
            }

            let block = ConfirmedBlock::new(
                BlockExecutionOutcome::default().with(make_child_block(chain.last().unwrap())),
            );

            chain.push(block);
        }

        let notification = BlockId::from_confirmed_block(chain.last().unwrap());

        for block in &chain {
            let cert = ConfirmedBlockCertificate::new(block.clone(), Round::Fast, vec![]);
            storage
                .write_blobs_and_certificate(&[], &cert)
                .await
                .unwrap();
        }

        (
            notification,
            chain.iter().map(|block| block.inner().hash()).collect(),
        )
    }

    #[tokio::test]
    async fn test_topological_sort_4() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let new_block_queue = NewBlockQueue {
            queue_rear: tx.clone(),
            queue_front: rx,
        };
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let (block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                .await?;
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let exporters_tracker = ExportersTracker::<
            ExporterCancellationSignal,
            DbStorage<MemoryDatabase, TestClock>,
        >::new(
            NodeOptions::default(),
            0,
            signal.clone(),
            exporter_storage.clone(),
            vec![],
        );
        let mut block_processor = BlockProcessor::new(
            exporters_tracker,
            block_processor_storage,
            new_block_queue,
            false,
        );
        let (block_id, state) = make_state_4(&storage).await;
        let _ = tx.send(block_id);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = block_processor.run_with_shutdown(signal, 5) => {},
        }

        for (index, expected_hash) in state.iter().enumerate() {
            let sorted_hash = exporter_storage
                .get_block_with_blob_ids(index)
                .await?
                .0
                .hash();
            assert_eq!(*expected_hash, sorted_hash);
        }

        Ok(())
    }

    // a simple single chain scenario with four blocks
    // a message to the same chain is sent from the second
    // block and reacieved by the last block.
    async fn make_state_4<S: Storage>(storage: &S) -> (BlockId, Vec<CryptoHash>) {
        let chain_id = ChainId(CryptoHash::test_hash("0"));

        let mut chain = Vec::new();

        for i in 0..4 {
            if i == 0 {
                let block = ConfirmedBlock::new(
                    BlockExecutionOutcome::default().with(make_first_block(chain_id)),
                );
                chain.push(block);
                continue;
            }

            let block = if i == 3 {
                let sender_block = chain.get(1).expect("we are at height 4");
                let incoming_bundle = IncomingBundle {
                    origin: chain_id,
                    bundle: MessageBundle {
                        height: sender_block.height(),
                        timestamp: Timestamp::now(),
                        certificate_hash: sender_block.inner().hash(),
                        transaction_index: 0,
                        messages: vec![],
                    },
                    action: MessageAction::Accept,
                };
                ConfirmedBlock::new(BlockExecutionOutcome::default().with(
                    make_child_block(chain.last().unwrap()).with_incoming_bundle(incoming_bundle),
                ))
            } else {
                ConfirmedBlock::new(
                    BlockExecutionOutcome::default().with(make_child_block(chain.last().unwrap())),
                )
            };

            chain.push(block);
        }

        let notification = BlockId::from_confirmed_block(chain.last().unwrap());

        for block in &chain {
            let cert = ConfirmedBlockCertificate::new(block.clone(), Round::Fast, vec![]);
            storage
                .write_blobs_and_certificate(&[], &cert)
                .await
                .unwrap();
        }

        (
            notification,
            chain.iter().map(|block| block.inner().hash()).collect(),
        )
    }

    // tests a simple scenario for a chain with two blocks
    // and some blobs
    #[tokio::test]
    async fn test_topological_sort_5() -> anyhow::Result<()> {
        let (tx, rx) = unbounded_channel();
        let new_block_queue = NewBlockQueue {
            queue_rear: tx.clone(),
            queue_front: rx,
        };
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let (block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                .await?;
        let token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(token.clone());
        let exporters_tracker = ExportersTracker::<
            ExporterCancellationSignal,
            DbStorage<MemoryDatabase, TestClock>,
        >::new(
            NodeOptions::default(),
            0,
            signal.clone(),
            exporter_storage.clone(),
            vec![],
        );
        let mut block_processor = BlockProcessor::new(
            exporters_tracker,
            block_processor_storage,
            new_block_queue,
            false,
        );
        let (block_id, expected_state) = make_simple_state_with_blobs(&storage).await;
        let _ = tx.send(block_id);

        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(10)) => {},
            _ = block_processor.run_with_shutdown(signal, 5) => {},
        }

        for (i, block_with_blobs) in expected_state.iter().enumerate() {
            let (actual_block, actual_blobs) = exporter_storage.get_block_with_blobs(i).await?;
            assert_eq!(actual_block.hash(), block_with_blobs.block_hash);
            assert!(!actual_blobs.is_empty());
            assert_eq!(actual_blobs.len(), block_with_blobs.blobs.len());
            assert!(actual_blobs
                .iter()
                .map(|blob| blob.id())
                .eq(block_with_blobs.blobs.iter().copied()));
        }

        Ok(())
    }
}
