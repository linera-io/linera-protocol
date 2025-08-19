// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::IntoFuture,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use futures::{stream::FuturesOrdered, StreamExt};
use linera_base::data_types::Blob;
use linera_chain::types::ConfirmedBlockCertificate;
use linera_rpc::NodeOptions;
use linera_service::config::DestinationId;
use linera_storage::Storage;
use tokio::{select, sync::mpsc::Sender, time::sleep};
use tonic::Streaming;

use super::indexer_api::Element;
use crate::{
    common::BlockId, runloops::indexer::client::IndexerClient, storage::ExporterStorage,
    ExporterError,
};

pub(crate) struct Exporter {
    options: NodeOptions,
    work_queue_size: usize,
    destination_id: DestinationId,
}

impl Exporter {
    pub(crate) fn new(
        destination_id: DestinationId,
        work_queue_size: usize,
        options: NodeOptions,
    ) -> Exporter {
        Self {
            options,
            destination_id,
            work_queue_size,
        }
    }

    pub(crate) async fn run_with_shutdown<S, F: IntoFuture<Output = ()>>(
        self,
        shutdown_signal: F,
        mut storage: ExporterStorage<S>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_signal_future = shutdown_signal.into_future();
        let mut pinned_shutdown_signal = Box::pin(shutdown_signal_future);

        let address = self.destination_id.address();
        let mut client = IndexerClient::new(address, self.options)?;
        let destination_state = storage.load_destination_state(&self.destination_id);

        tracing::info!(start_index=&destination_state.load(Ordering::SeqCst), indexer_address=%address, "starting indexer exporter");

        loop {
            let (outgoing_stream, incoming_stream) =
                client.setup_indexer_client(self.work_queue_size).await?;
            let mut streamer = ExportTaskQueue::new(
                self.work_queue_size,
                destination_state.load(Ordering::Acquire) as usize,
                outgoing_stream,
                storage.clone(),
            );

            let mut acknowledgement_task =
                AcknowledgementTask::new(incoming_stream, destination_state.clone());

            select! {

                biased;

                _ = &mut pinned_shutdown_signal => {break},

                res = streamer.run() => {
                    if let Err(e) = res {
                        tracing::error!("unexpected error: {e}, re-trying to establish a stream");
                        sleep(Duration::from_secs(1)).await;
                    }
                },

                res = acknowledgement_task.run() => {
                    match res {
                        Err(e) => {
                            tracing::error!("unexpected error: {e}, re-trying to establish a stream");
                        }

                        Ok(_) => {
                            tracing::error!("stream closed unexpectedly, retrying to establish a stream");
                        }
                    }

                    sleep(Duration::from_secs(1)).await;
                },

            }
        }

        Ok(())
    }
}

struct AcknowledgementTask {
    incoming: Streaming<()>,
    destination_state: Arc<AtomicU64>,
}

impl AcknowledgementTask {
    fn new(incoming: Streaming<()>, destination_state: Arc<AtomicU64>) -> Self {
        Self {
            incoming,
            destination_state,
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while self.incoming.message().await?.is_some() {
            self.increment_destination_state();
        }

        Ok(())
    }

    fn increment_destination_state(&self) {
        let _ = self.destination_state.fetch_add(1, Ordering::Release);
    }
}

struct ExportTaskQueue<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    queue_size: usize,
    start_height: usize,
    buffer: CanonicalBlockStream,
    storage: ExporterStorage<S>,
}

impl<S> ExportTaskQueue<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        queue_size: usize,
        start_height: usize,
        sender: CanonicalBlockStream,
        storage: ExporterStorage<S>,
    ) -> ExportTaskQueue<S> {
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
        while futures.len() < self.queue_size {
            futures.push_back(self.get_block_with_blobs_task(index));
            index += 1;
        }

        while let Some((block, blobs)) = futures.next().await.transpose()? {
            for blob in blobs {
                tracing::info!(
                    blob_id=?blob.id(),
                    "dispatching blob"
                );
                self.buffer.send(blob.try_into().unwrap()).await?
            }

            let block_id = BlockId::from_confirmed_block(block.value());
            tracing::info!(?block_id, "dispatching block");
            self.buffer.send(block.try_into().unwrap()).await?;

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
