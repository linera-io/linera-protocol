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
use tokio::{
    select,
    sync::mpsc::Sender,
    time::{interval_at, sleep, Instant},
};
use tonic::Streaming;

use super::indexer_api::Element;
use crate::{
    common::BlockId, runloops::indexer::client::IndexerClient, storage::ExporterStorage,
    ExporterError,
};

/// On reconnect, resume from a few heights before the last acked height.
/// Since the destination is idempotent, replaying a small window is safe
/// and prevents permanent stalls when ACKs are lost during stream resets.
const REWIND_BLOCKS: usize = 3;

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
        storage: ExporterStorage<S>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_signal_future = shutdown_signal.into_future();
        let mut pinned_shutdown_signal = Box::pin(shutdown_signal_future);

        let address = self.destination_id.address();
        let mut client = IndexerClient::new(address, self.options)?;
        let destination_state = storage.load_destination_state(&self.destination_id);

        tracing::info!(
            start_index=&destination_state.load(Ordering::SeqCst),
            indexer_address=%address,
            "starting indexer exporter"
        );

        loop {
            let (outgoing_stream, incoming_stream) =
                client.setup_indexer_client(self.work_queue_size).await?;

            let acked_height = destination_state.load(Ordering::Acquire) as usize;
            let start_height = acked_height.saturating_sub(REWIND_BLOCKS);

            tracing::info!(
                acked_height,
                start_height,
                "stream established, resuming export"
            );

            let mut streamer = ExportTaskQueue::new(
                self.work_queue_size,
                start_height,
                outgoing_stream,
                storage.clone()?,
            );

            let mut acknowledgement_task =
                AcknowledgementTask::new(incoming_stream, destination_state.clone());

            // Pin the streamer future so it survives the select!. When
            // the ack branch wins, select! only drops the &mut borrow,
            // not the future itself, so we can keep polling it.
            let streamer_fut = streamer.run();
            tokio::pin!(streamer_fut);

            select! {

                biased;

                _ = &mut pinned_shutdown_signal => {break},

                res = &mut streamer_fut => {
                    if let Err(error) = res {
                        tracing::error!(?error, "exporter stream error. re-trying to establish a stream");
                        client = IndexerClient::new(address, self.options)?;
                        sleep(Duration::from_millis(500)).await;
                    }
                },

                res = acknowledgement_task.run() => {
                    match res {
                        Err(error) => {
                            tracing::error!(?error, "ack stream error. letting export stream drain before reconnecting");
                        }

                        Ok(_) => {
                            tracing::error!("ack stream ended unexpectedly. letting export stream drain before reconnecting");
                        }
                    }

                    // The ack stream died but the export streamer may still
                    // have in-flight sends. Let it finish — it will error
                    // out when it next tries to send on the dead connection
                    // (at most one keepalive period, ~30s).
                    select! {
                        biased;

                        _ = &mut pinned_shutdown_signal => {break},

                        res = &mut streamer_fut => {
                            if let Err(error) = res {
                                tracing::error!(?error, "exporter stream error after ack stream failure");
                            }
                        },
                    }

                    client = IndexerClient::new(address, self.options)?;
                    sleep(Duration::from_millis(500)).await;
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
            let new_height = self.destination_state.fetch_add(1, Ordering::Release) + 1;
            tracing::info!(
                acked_height = new_height,
                "block acknowledged by destination"
            );
        }

        Ok(())
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
        let keepalive_period = Duration::from_secs(30);
        let mut index = self.start_height;
        let mut futures = FuturesOrdered::new();
        while futures.len() < self.queue_size {
            futures.push_back(self.get_block_with_blobs_task(index));
            index += 1;
        }

        let mut keepalive = interval_at(Instant::now() + keepalive_period, keepalive_period);

        loop {
            select! {
                result = futures.next() => {
                    match result.transpose()? {
                        Some((block, blobs)) => {
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
                            keepalive.reset();
                        }
                        None => return Ok(()),
                    }
                }

                _ = keepalive.tick() => {
                    tracing::trace!("sending keepalive to indexer");
                    #[cfg(with_metrics)]
                    crate::metrics::KEEPALIVES_SENT.inc();
                    self.buffer.send(Element { payload: None }).await?;
                }
            }
        }
    }

    async fn get_block_with_blobs_task(
        &self,
        index: usize,
    ) -> Result<(Arc<ConfirmedBlockCertificate>, Vec<Arc<Blob>>), ExporterError> {
        let mut retries = 0u32;
        loop {
            match self.storage.get_block_with_blobs(index).await {
                Ok(res) => return Ok(res),
                Err(ExporterError::UnprocessedBlock) => {
                    retries += 1;
                    if retries == 1 {
                        tracing::info!(index, "waiting for block to be processed");
                    } else if retries % 60 == 0 {
                        tracing::warn!(index, retries, "still waiting for block to be processed");
                    }
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Err(e) => return Err(e),
            }
        }
    }
}

type CanonicalBlockStream = Sender<Element>;
