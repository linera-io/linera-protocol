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

use futures::{future::try_join_all, stream::FuturesOrdered};
use linera_base::identifiers::BlobId;
use linera_chain::types::ConfirmedBlockCertificate;
use linera_core::node::{
    CrossChainMessageDelivery, NodeError, ValidatorNode, ValidatorNodeProvider,
};
use linera_rpc::grpc::{GrpcClient, GrpcNodeProvider};
use linera_service::config::DestinationId;
use linera_storage::Storage;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio_stream::StreamExt;

use crate::{
    common::{BlockId, ExporterError},
    storage::ExporterStorage,
};

pub(crate) struct Exporter {
    node_provider: Arc<GrpcNodeProvider>,
    destination_id: DestinationId,
    work_queue_size: usize,
}

impl Exporter {
    pub(super) fn new(
        destination_id: DestinationId,
        node_provider: Arc<GrpcNodeProvider>,
        work_queue_size: usize,
    ) -> Self {
        Self {
            node_provider,
            destination_id,
            work_queue_size,
        }
    }

    pub(super) async fn run_with_shutdown<S, F: IntoFuture<Output = ()>>(
        self,
        shutdown_signal: F,
        mut storage: ExporterStorage<S>,
    ) -> anyhow::Result<()>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let address = self.destination_id.address().to_owned();
        let destination_state = storage.load_destination_state(&self.destination_id);

        let node = self.node_provider.make_node(&address)?;

        let (mut task_queue, task_receiver) = TaskQueue::new(
            self.work_queue_size,
            destination_state.load(Ordering::Acquire) as usize,
            storage.clone(),
        );

        let export_task = ExportTask::new(node, storage.clone(), destination_state);

        tokio::select! {

            biased;

            _ = shutdown_signal => {},

            res = task_queue.run() => res?,

            res = export_task.run(task_receiver) => res?,

        };

        Ok(())
    }
}

struct ExportTask<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    node: GrpcClient,
    storage: ExporterStorage<S>,
    destination_state: Arc<AtomicU64>,
}

impl<S> ExportTask<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    fn new(
        node: GrpcClient,
        storage: ExporterStorage<S>,
        destination_state: Arc<AtomicU64>,
    ) -> ExportTask<S> {
        ExportTask {
            node,
            storage,
            destination_state,
        }
    }

    async fn run(
        &self,
        mut receiver: Receiver<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>)>,
    ) -> anyhow::Result<()> {
        while let Some((block, blobs_ids)) = receiver.recv().await {
            #[cfg(with_metrics)]
            crate::metrics::VALIDATOR_EXPORTER_QUEUE_LENGTH
                .with_label_values(&[self.node.address()])
                .set(receiver.len() as i64);
            match self.dispatch_block((*block).clone()).await {
                Ok(_) => {}

                Err(NodeError::BlobsNotFound(blobs_to_maybe_send)) => {
                    let blobs = blobs_ids
                        .into_iter()
                        .filter(|id| blobs_to_maybe_send.contains(id))
                        .collect();
                    self.upload_blobs(blobs).await?;
                    self.dispatch_block((*block).clone()).await?
                }

                Err(e) => Err(e)?,
            }

            self.increment_destination_state();
        }

        Ok(())
    }

    fn increment_destination_state(&self) {
        let _ = self.destination_state.fetch_add(1, Ordering::Release);
        #[cfg(with_metrics)]
        crate::metrics::DESTINATION_STATE_COUNTER
            .with_label_values(&[self.node.address()])
            .inc();
    }

    async fn upload_blobs(&self, blobs: Vec<BlobId>) -> anyhow::Result<()> {
        let tasks = blobs.iter().map(|id| async {
            match self.storage.get_blob(*id).await {
                Err(e) => Err(e),
                Ok(blob) => {
                    tracing::info!(
                        "dispatching blob with id: {:#?} from linera exporter",
                        blob.id()
                    );
                    #[cfg(with_metrics)]
                    let start = linera_base::time::Instant::now();
                    let result = self
                        .node
                        .upload_blob((*blob).clone().into())
                        .await
                        .map(|_| ())
                        .map_err(|e| ExporterError::GenericError(e.into()));
                    #[cfg(with_metrics)]
                    crate::metrics::DISPATCH_BLOB_HISTOGRAM
                        .with_label_values(&[self.node.address()])
                        .observe(start.elapsed().as_secs_f64() * 1000.0);
                    result
                }
            }
        });

        let _ = try_join_all(tasks).await?;

        Ok(())
    }

    async fn dispatch_block(
        &self,
        certificate: ConfirmedBlockCertificate,
    ) -> Result<(), NodeError> {
        let delivery = CrossChainMessageDelivery::NonBlocking;
        let block_id = BlockId::from_confirmed_block(certificate.value());
        tracing::info!(?block_id, "dispatching block");
        #[cfg(with_metrics)]
        let start = linera_base::time::Instant::now();
        match self
            .node
            .handle_confirmed_certificate(certificate, delivery)
            .await
        {
            Ok(_) => {
                #[cfg(with_metrics)]
                crate::metrics::DISPATCH_BLOCK_HISTOGRAM
                    .with_label_values(&[self.node.address()])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
            }
            Err(e) => {
                tracing::error!(error=%e, ?block_id, "error when dispatching block");
                #[cfg(with_metrics)]
                crate::metrics::DISPATCH_BLOCK_HISTOGRAM
                    .with_label_values(&[self.node.address()])
                    .observe(start.elapsed().as_secs_f64() * 1000.0);
                Err(e)?
            }
        }

        Ok(())
    }
}

struct TaskQueue<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    queue_size: usize,
    start_height: usize,
    storage: ExporterStorage<S>,
    buffer: Sender<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>)>,
}

impl<S> TaskQueue<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    #[expect(clippy::type_complexity)]
    fn new(
        queue_size: usize,
        start_height: usize,
        storage: ExporterStorage<S>,
    ) -> (
        TaskQueue<S>,
        Receiver<(Arc<ConfirmedBlockCertificate>, Vec<BlobId>)>,
    ) {
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
        while futures.len() < self.queue_size {
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
