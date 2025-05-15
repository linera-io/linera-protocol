// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, sync::Arc, time::Duration};

use futures::{stream::FuturesOrdered, StreamExt};
use linera_base::{data_types::Blob, ensure};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_client::config::{Destination, DestinationId, DestinationKind};
use linera_rpc::NodeOptions;
use linera_storage::Storage;
use tokio::{select, sync::mpsc::Sender, time::sleep};
use tonic::Streaming;

use super::indexer_api::Element;
use crate::{
    common::BlockId, dispatch, runloops::indexer::client::IndexerClient, storage::ExporterStorage,
    ExporterError,
};

pub(crate) struct Exporter<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    options: NodeOptions,
    work_queue_size: usize,
    storage: ExporterStorage<S>,
    destination_id: DestinationId,
    destination_config: Destination,
}

impl<S> Exporter<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(
        options: NodeOptions,
        work_queue_size: usize,
        storage: ExporterStorage<S>,
        destination_id: DestinationId,
        destination_config: Destination,
    ) -> Exporter<S> {
        Self {
            options,
            storage,
            destination_id,
            work_queue_size,
            destination_config,
        }
    }

    pub(crate) async fn run_with_shutdown<F: IntoFuture<Output = ()>>(
        self,
        signal: F,
    ) -> anyhow::Result<()> {
        ensure!(
            DestinationKind::Indexer == self.destination_config.kind,
            ExporterError::DestinationError
        );

        let furure = signal.into_future();
        let mut pinned = Box::pin(furure);

        let address = self.destination_config.indexer_address();
        let mut client = IndexerClient::new(&address, self.options)?;

        loop {
            let (outgoing_stream, incoming_stream) = client
                .synchronize_long_lived_stream(self.work_queue_size)
                .await?;
            let mut streamer = ExportTaskQueue::new(
                self.work_queue_size,
                outgoing_stream,
                self.destination_id,
                &self.storage,
            );
            let mut acknowledgement_task =
                AcknowledgementTask::new(incoming_stream, self.destination_id, &self.storage);

            select! {

                biased;

                _ = &mut pinned => {break},

                res = streamer.run() => {
                    if let Err(e) = res {
                        tracing::error!("unexpected error: {e}, re-trying to establish a stream");
                        sleep(Duration::from_secs(1)).await;
                    }
                },

                res = acknowledgement_task.run() => {
                    if let Err(e) = res {
                        tracing::error!("unexpected error: {e}, re-trying to establish a stream");
                        sleep(Duration::from_secs(1)).await;
                    }
                },

            }
        }

        Ok(())
    }
}

struct AcknowledgementTask<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    incoming: Streaming<()>,
    destination_id: DestinationId,
    storage: &'a ExporterStorage<S>,
}

impl<'a, S> AcknowledgementTask<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        incoming: Streaming<()>,
        destination_id: DestinationId,
        storage: &'a ExporterStorage<S>,
    ) -> Self {
        Self {
            incoming,
            destination_id,
            storage,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while self.incoming.message().await?.is_some() {
            self.storage.increment_destination(self.destination_id);
        }

        tracing::error!("stream closed unexpectedly, retrying to establish a stream");
        sleep(Duration::from_secs(1)).await;
        Ok(())
    }
}

struct ExportTaskQueue<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    queue_size: usize,
    start_height: usize,
    buffer: CanonicalBlockStream,
    storage: &'a ExporterStorage<S>,
}

impl<'a, S> ExportTaskQueue<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        queue_size: usize,
        sender: CanonicalBlockStream,
        destination_id: DestinationId,
        storage: &'a ExporterStorage<S>,
    ) -> ExportTaskQueue<'a, S> {
        let start_height = storage.load_destination_state(destination_id) as usize;

        Self {
            queue_size,
            start_height,
            storage,
            buffer: sender,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut index = self.start_height;
        let mut futures = FuturesOrdered::new();
        while futures.len() != self.queue_size {
            futures.push_back(self.get_block_with_blobs_task(index));
            index += 1;
        }

        while let Some((block, blobs)) = futures.next().await.transpose()? {
            for blob in blobs {
                let blob_id = blob.id();
                dispatch!(
                    |payload| self.buffer.send(payload),
                    log = blob_id,
                    blob.into()
                )?;
            }

            let block_id = BlockId::from_confirmed_block(block.value());
            dispatch!(
                |payload| self.buffer.send(payload),
                log = block_id,
                block.try_into().unwrap()
            )?;

            futures.push_back(self.get_block_with_blobs_task(index));
            index += 1;
        }

        Ok(())
    }

    async fn get_block_with_blobs_task(
        &self,
        index: usize,
    ) -> Result<(Arc<ConfirmedBlockCertificate>, Vec<Arc<Blob>>), ExporterError> {
        loop {
            match self.storage.get_block_with_blobs(index).await {
                Ok(res) => return Ok(res),
                Err(ExporterError::UnprocessedBlock) => {
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Err(e) => return Err(e),
            }
        }
    }
}

type CanonicalBlockStream = Sender<Element>;
