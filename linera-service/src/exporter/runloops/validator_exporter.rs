// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{future::IntoFuture, sync::Arc, time::Duration};

use futures::{future::try_join_all, stream::FuturesOrdered};
use linera_base::{ensure, identifiers::BlobId};
use linera_chain::types::ConfirmedBlockCertificate;
use linera_client::config::{Destination, DestinationId, DestinationKind};
use linera_core::node::{
    CrossChainMessageDelivery, NodeError, ValidatorNode, ValidatorNodeProvider,
};
use linera_rpc::grpc::{GrpcClient, GrpcNodeProvider};
use linera_storage::Storage;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use crate::{
    common::{BlockId, ExporterError},
    dispatch,
    storage::ExporterStorage,
};

pub(crate) struct Exporter<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    node_provider: Arc<GrpcNodeProvider>,
    destination_id: DestinationId,
    storage: ExporterStorage<S>,
    destination_config: Destination,
    work_queue_size: usize,
}

impl<S> Exporter<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub(super) fn new(
        node_provider: Arc<GrpcNodeProvider>,
        destination_id: DestinationId,
        storage: ExporterStorage<S>,
        destination_config: Destination,
        work_queue_size: usize,
    ) -> Self {
        Self {
            node_provider,
            destination_id,
            storage,
            destination_config,
            work_queue_size,
        }
    }

    pub(super) async fn run_with_shutdown<F: IntoFuture<Output = ()>>(
        self,
        shutdown_signal: F,
    ) -> anyhow::Result<()> {
        ensure!(
            DestinationKind::Validator == self.destination_config.kind,
            ExporterError::DestinationError
        );

        let address = self.destination_config.validator_address();
        let node = self.node_provider.make_node(&address)?;

        let export_task = ExportTask::new(node, self.destination_id, &self.storage);
        let (mut task_queue, task_receiver) =
            TaskQueue::new(self.work_queue_size, self.destination_id, &self.storage);

        tokio::select! {

            biased;

            _ = shutdown_signal => {},

            res = task_queue.run() => res?,

            res = export_task.run(task_receiver) => res?,

        };

        Ok(())
    }
}

struct ExportTask<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    node: GrpcClient,
    destination_id: DestinationId,
    storage: &'a ExporterStorage<S>,
}

impl<'a, S> ExportTask<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        node: GrpcClient,
        destination_id: DestinationId,
        storage: &'a ExporterStorage<S>,
    ) -> ExportTask<'a, S> {
        ExportTask {
            node,
            destination_id,
            storage,
        }
    }

    async fn run(
        &self,
        mut receiver: Receiver<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>)>,
    ) -> anyhow::Result<()> {
        let delivery = CrossChainMessageDelivery::NonBlocking;
        while let Some((block, blobs_ids)) = receiver.recv().await {
            let block_id = BlockId::from_confirmed_block(block.value());
            let method = |certificate, delivery| {
                self.node
                    .handle_confirmed_certificate(certificate, delivery)
            };

            match dispatch!(method, log = block_id, (*block).clone(), delivery) {
                Ok(_) => {}
                Err(NodeError::BlobsNotFound(blobs_to_maybe_send)) => {
                    let blobs = blobs_ids
                        .into_iter()
                        .filter(|id| blobs_to_maybe_send.contains(id))
                        .collect();
                    let method = |blobs| self.upload_blobs(blobs);
                    dispatch!(method, log = blobs, blobs)?;
                }
                Err(e) => Err(e)?,
            }

            self.storage.increment_destination(self.destination_id);
        }

        Ok(())
    }

    async fn upload_blobs(&self, blobs: Vec<BlobId>) -> anyhow::Result<()> {
        let tasks = blobs.iter().map(|id| async {
            match self.storage.get_blob(*id).await {
                Err(e) => Err(e),
                Ok(blob) => self
                    .node
                    .upload_blob((*blob).clone().into())
                    .await
                    .map_err(|e| ExporterError::GenericError(e.into()))
                    .map(|_| ()),
            }
        });

        let _ = try_join_all(tasks).await?;

        Ok(())
    }
}

struct TaskQueue<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    queue_size: usize,
    start_height: usize,
    storage: &'a ExporterStorage<S>,
    buffer: Sender<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>)>,
}

impl<'a, S> TaskQueue<'a, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[expect(clippy::type_complexity)]
    fn new(
        queue_size: usize,
        destination_id: DestinationId,
        storage: &'a ExporterStorage<S>,
    ) -> (
        TaskQueue<'a, S>,
        Receiver<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>)>,
    ) {
        let start_height = storage.load_destination_state(destination_id) as usize;
        let (sender, receiver) = tokio::sync::mpsc::channel(queue_size);

        let queue = Self {
            queue_size,
            start_height,
            storage,
            buffer: sender,
        };

        (queue, receiver)
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let mut index = self.start_height;
        let mut futures = FuturesOrdered::new();
        while futures.len() != self.queue_size {
            futures.push_back(self.get_block_task(index));
            index += 1;
        }

        while let Some(certificate) = futures.next().await.transpose()? {
            let _ = self.buffer.send(certificate).await;
            futures.push_back(self.get_block_task(index));
            index += 1;
        }

        Ok(())
    }

    async fn get_block_task(
        &self,
        index: usize,
    ) -> Result<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>), ExporterError> {
        loop {
            match self.storage.get_block_with_blob_ids(index).await {
                Ok(block_with_blobs_ids) => return Ok(block_with_blobs_ids),
                Err(ExporterError::UnprocessedBlock) => {
                    tokio::time::sleep(Duration::from_secs(1)).await
                }
                Err(e) => return Err(e),
            }
        }
    }
}
