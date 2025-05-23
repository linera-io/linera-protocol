// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, num::NonZeroU8, sync::Arc, time::Duration};

use anyhow::Context;
use chrono::Utc;
use futures::stream::{FuturesOrdered, FuturesUnordered};
use linera_base::data_types::Blob;
use linera_chain::types::ConfirmedBlockCertificate;
use linera_client::config::{DestinationId, WalrusBatchSize, WalrusConfig, WalrusStoreConfig};
use linera_storage::Storage;
use serde::Serialize;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;
use walrus_core::BlobId as WalrusBlobId;

use super::{client::WalrusClient, logger::RollingFileLogger};
use crate::{storage::ExporterStorage, ExporterError};

pub(crate) struct Exporter<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    work_queue_size: usize,
    storage: ExporterStorage<S>,
    destination_id: DestinationId,
    config: WalrusConfig<WalrusStoreConfig>,
}

impl<S> Exporter<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        destination_id: DestinationId,
        work_queue_size: usize,
        storage: ExporterStorage<S>,
        config: WalrusConfig<WalrusStoreConfig>,
    ) -> Self {
        Self {
            work_queue_size,
            destination_id,
            storage,
            config,
        }
    }

    pub(crate) async fn run_with_shutdown<F: IntoFuture<Output = ()>>(
        self,
        signal: F,
    ) -> anyhow::Result<()> {
        tracing::info!("starting walrus exporter");

        let export_task = ExportTask::new(&self.config, self.destination_id, &self.storage).await?;
        let (queue_task, task_reciever) = TaskQueue::new(
            self.config.behaviour.batch_size,
            self.config.behaviour.batch_timeout,
            self.work_queue_size,
            self.destination_id,
            &self.storage,
        );

        let mut logger = RollingFileLogger::new(
            self.config.log.as_path(),
            self.config.behaviour.max_log_size.get().into(),
        )
        .await
        .with_context(|| "unable to setup a logger")?;

        // just to make this cancel safe, for now
        let mut export_task_future = FuturesUnordered::new();
        export_task_future.push(export_task.run(task_reciever, &mut logger));

        tokio::select! {

            biased;

            _ = signal => {},

            res = queue_task.run() => res?,

            Some(res) = export_task_future.next() => res?,

        }

        drop(queue_task);
        // gracefully handle the remaining exports
        while export_task_future.next().await.transpose()?.is_some() {}

        drop(export_task_future);

        logger.flush_logs().await
    }
}

struct ExportTask<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    client: WalrusClient,
    destination_id: DestinationId,
    storage: &'a ExporterStorage<S>,
}

struct TaskQueue<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    queue_size: usize,
    start_height: usize,
    buffer: Sender<(Vec<u8>, u64)>,
    batch_timeout: NonZeroU8,
    batch_size: WalrusBatchSize,
    storage: &'a ExporterStorage<S>,
}

impl<'a, S> ExportTask<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn new(
        config: &WalrusConfig<WalrusStoreConfig>,
        destination_id: DestinationId,
        storage: &'a ExporterStorage<S>,
    ) -> anyhow::Result<Self> {
        loop {
            match WalrusClient::new(config).await {
                Ok(client) => {
                    return Ok(Self {
                        client,
                        destination_id,
                        storage,
                    })
                }

                Err(e) => {
                    let retry_duration = Duration::from_secs(1);
                    tracing::error!("unable to create a walrus client because of this recieved error: {e}, re-trying in {retry_duration:#?} seconds");
                    tokio::time::sleep(retry_duration).await;
                }
            }
        }
    }

    async fn run(
        &self,
        mut receiver: Receiver<(Vec<u8>, u64)>,
        logger: &mut RollingFileLogger,
    ) -> anyhow::Result<()> {
        let mut last = 0;
        while let Some((batch, next)) = receiver.recv().await {
            let blob_id = self.client.dispatch(batch.as_ref()).await?;
            Self::log(logger, blob_id)
                .await
                .with_context(|| format!("unable to log the blob_id: {}", blob_id))?;

            for _ in 0..next - last {
                self.storage.increment_destination(self.destination_id);
            }

            last = next;
        }

        Ok(())
    }

    async fn log(logger: &mut RollingFileLogger, blob_id: WalrusBlobId) -> anyhow::Result<()> {
        let now = Utc::now();
        let to_log = format!(
            "{} successfully stored blob with id: {} on walrus",
            now, blob_id
        );
        logger.log(to_log).await
    }
}

impl<'a, S> TaskQueue<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        batch_size: WalrusBatchSize,
        batch_timeout: NonZeroU8,
        queue_size: usize,
        destination_id: DestinationId,
        storage: &'a ExporterStorage<S>,
    ) -> (Self, Receiver<(Vec<u8>, u64)>) {
        let start_height = storage.load_destination_state(destination_id) as usize;
        let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);

        let queue = Self {
            batch_timeout,
            queue_size,
            start_height,
            storage,
            buffer: sender,
            batch_size,
        };

        (queue, receiver)
    }

    async fn run(&self) -> anyhow::Result<()> {
        let mut index = self.start_height;
        let mut futures = FuturesOrdered::new();
        while futures.len() != self.queue_size {
            futures.push_back(self.get_block_with_blobs_task(index));
            index += 1;
        }

        let timeout = Duration::from_secs(self.batch_timeout.get().into());
        let mut interval = tokio::time::sleep(timeout);

        let mut batch_builder = BatchBuilder::new(self.batch_size);

        loop {
            tokio::select! {

                biased;

                Some(res) = futures.next() => {
                    let (block, blobs) = res?;
                    let batches = batch_builder.add_to_batch(block, blobs)?;
                    for batch_with_maybe_index in batches {
                        self.buffer.send(batch_with_maybe_index).await.expect("this should not fail");
                    }

                    futures.push_back(self.get_block_with_blobs_task(index));
                    interval = tokio::time::sleep(timeout);
                    index += 1;
                },

                _ = interval => {
                    if let Some(batch) = batch_builder.yield_batch() {
                        self.buffer.send(batch).await.expect("this should not fail");
                    }

                    interval = tokio::time::sleep(timeout);
                },

            }
        }
    }

    async fn get_block_with_blobs_task(
        &self,
        index: usize,
    ) -> Result<(Arc<ConfirmedBlockCertificate>, Vec<Arc<Blob>>), ExporterError> {
        loop {
            match self.storage.get_block_with_blobs(index).await {
                Ok(block_with_blobs) => return Ok(block_with_blobs),
                Err(ExporterError::UnprocessedBlock) => {
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Err(e) => return Err(e),
            }
        }
    }
}

struct BatchBuilder {
    current_block: u64,
    walrus_blob_buffer: Vec<u8>,
    max_batch_size: WalrusBatchSize,
}

impl BatchBuilder {
    fn new(size: WalrusBatchSize) -> Self {
        Self {
            current_block: 0,
            walrus_blob_buffer: vec![],
            max_batch_size: size,
        }
    }

    fn add_to_batch(
        &mut self,
        block: Arc<ConfirmedBlockCertificate>,
        blobs: Vec<Arc<Blob>>,
    ) -> anyhow::Result<Vec<(Vec<u8>, u64)>> {
        let mut batches = vec![];

        // by manually passing variant index
        // can avoid cloning
        match self.max_batch_size {
            WalrusBatchSize::Weight(weight) => {
                let mut add_to_inner_batch = |mut buffer, increment_index| {
                    self.walrus_blob_buffer.append(&mut buffer);

                    if increment_index {
                        self.current_block += 1;
                    }

                    if self.walrus_blob_buffer.len() > weight.get() as usize * 1024 * 1024 {
                        let batch = std::mem::take(&mut self.walrus_blob_buffer);
                        batches.push((batch, self.current_block));
                    }
                };

                for blob in blobs {
                    let variant_index = 0;
                    let mut buffer = Vec::new();
                    bcs::serialize_into(&mut buffer, &variant_index)?;
                    bcs::serialize_into(&mut buffer, blob.as_ref())?;
                    add_to_inner_batch(buffer, false);
                }

                let variant_index = 1;
                let mut buffer = Vec::new();
                bcs::serialize_into(&mut buffer, &variant_index)?;
                bcs::serialize_into(&mut buffer, block.as_ref())?;
                add_to_inner_batch(buffer, true);
            }

            WalrusBatchSize::Blocks(max_block) => {
                let variant_index = 2;
                bcs::serialize_into(&mut self.walrus_blob_buffer, &variant_index)?;
                for blob in blobs {
                    bcs::serialize_into(&mut self.walrus_blob_buffer, blob.as_ref())?;
                }

                bcs::serialize_into(&mut self.walrus_blob_buffer, &block.as_ref())?;

                self.current_block += 1;

                if self.current_block % max_block.get() as u64 == 0 {
                    let batch = std::mem::take(&mut self.walrus_blob_buffer);
                    batches.push((batch, self.current_block));
                }
            }
        }

        Ok(batches)
    }

    // forcefully yield a batch
    // if this batch is not sent before some timeout
    fn yield_batch(&mut self) -> Option<(Vec<u8>, u64)> {
        if self.walrus_blob_buffer.is_empty() {
            return None;
        }

        Some((
            std::mem::take(&mut self.walrus_blob_buffer),
            self.current_block,
        ))
    }
}

// just a helper container to build the next item to serialize into a batch
#[allow(dead_code)]
#[derive(Serialize)]
struct BatchBlock {
    blobs: Vec<Blob>,
    block: ConfirmedBlockCertificate,
}

#[allow(dead_code)]
#[derive(Serialize)]
enum BatchElement {
    Blob(Blob),
    BatchBlock(BatchBlock),
    Block(ConfirmedBlockCertificate),
}

#[cfg(test)]
mod tests {
    use std::{num::NonZeroU8, sync::Arc};

    use anyhow::Result;
    use linera_base::{
        data_types::{Blob, Round},
        identifiers::ChainId,
    };
    use linera_chain::{
        data_types::BlockExecutionOutcome,
        test::make_first_block,
        types::{ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_client::config::WalrusBatchSize;

    use crate::runloops::walrus::walrus_exporter::BatchBuilder;

    #[test]
    fn test_new_builder() {
        let size = WalrusBatchSize::Blocks(NonZeroU8::new(2).unwrap());
        let builder = BatchBuilder::new(size);
        assert_eq!(builder.current_block, 0);
        assert!(builder.walrus_blob_buffer.is_empty());
    }

    #[test]
    fn test_weight_batch_not_exceeding() -> Result<()> {
        let size = WalrusBatchSize::Weight(NonZeroU8::new(1).unwrap());
        let mut builder = BatchBuilder::new(size);

        let block = BlockExecutionOutcome::default().with(make_first_block(ChainId::default()));
        let block = Arc::new(ConfirmedBlockCertificate::new(
            ConfirmedBlock::new(block),
            Round::Fast,
            vec![],
        ));
        let blob = Arc::new(Blob::new_data(*b"bytes"));

        let batches = builder.add_to_batch(block.clone(), vec![blob.clone()])?;

        assert!(batches.is_empty());

        if let Some((buffer, index)) = builder.yield_batch() {
            assert!(!buffer.is_empty());
            assert_eq!(index, 1);
        } else {
            panic!("expected buffer from yield_batch");
        }

        Ok(())
    }

    #[test]
    fn test_weight_batch_exceeding() -> Result<()> {
        let size = WalrusBatchSize::Weight(NonZeroU8::new(1).unwrap());
        let mut builder = BatchBuilder::new(size);

        // slightly higher than the limit converted
        let large_byte_buffer = vec![0u8; 1_100_000];

        let block = BlockExecutionOutcome::default().with(make_first_block(ChainId::default()));
        let block = Arc::new(ConfirmedBlockCertificate::new(
            ConfirmedBlock::new(block),
            Round::Fast,
            vec![],
        ));
        let blob = Arc::new(Blob::new_data(large_byte_buffer));

        let batches = builder.add_to_batch(block.clone(), vec![blob.clone()])?;

        assert_eq!(batches.len(), 1);
        let (data, index) = &batches[0];
        assert!(!data.is_empty());
        assert_eq!(*index, 0);

        let batches = builder.add_to_batch(block.clone(), vec![blob.clone()])?;

        assert_eq!(batches.len(), 1);
        let (data, index) = &batches[0];
        assert!(!data.is_empty());
        assert_eq!(*index, 1);

        if let Some((buffer, index)) = builder.yield_batch() {
            assert!(!buffer.is_empty());
            assert_eq!(index, 2);
        } else {
            panic!("expected buffer from yield_batch");
        }

        Ok(())
    }

    #[test]
    fn test_blocks_batch_trigger() -> Result<()> {
        let size = WalrusBatchSize::Blocks(NonZeroU8::new(2).unwrap());
        let mut builder = BatchBuilder::new(size);

        let block = BlockExecutionOutcome::default().with(make_first_block(ChainId::default()));
        let block = Arc::new(ConfirmedBlockCertificate::new(
            ConfirmedBlock::new(block),
            Round::Fast,
            vec![],
        ));
        let blob = Arc::new(Blob::new_data(*b"bytes"));

        let batches = builder.add_to_batch(block.clone(), vec![blob.clone()])?;
        assert!(batches.is_empty());

        let batches = builder.add_to_batch(block.clone(), vec![blob.clone()])?;
        assert_eq!(batches.len(), 1);
        let (data, index) = &batches[0];
        assert!(!data.is_empty());
        assert_eq!(*index, 2);

        let batches = builder.add_to_batch(block.clone(), vec![blob.clone()])?;
        assert!(batches.is_empty());

        let batch = builder.yield_batch().expect("expected a pending batch");
        let (data, index) = &batch;
        assert!(!data.is_empty());
        assert_eq!(*index, 3);

        assert!(builder.yield_batch().is_none());

        Ok(())
    }

    #[test]
    fn test_yield_empty() {
        let mut builder = BatchBuilder::new(WalrusBatchSize::Blocks(NonZeroU8::new(1).unwrap()));
        assert!(builder.yield_batch().is_none());
    }
}
