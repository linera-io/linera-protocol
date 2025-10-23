// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    future::{Future, IntoFuture},
    path::Path,
    sync::Arc,
};

use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_service::config::{Destination, DestinationId, DestinationKind};
use linera_storage::Storage;
use tokio::task::{AbortHandle, JoinSet};

use crate::{runloops::logging_exporter::LoggingExporter, storage::ExporterStorage};

/// This type manages tasks like spawning different exporters on the different
/// threads, discarding the committees and joining every thread properly at the
/// end for graceful shutdown of the process.
pub(super) struct ExportersTracker<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    exporters_builder: ExporterBuilder<F>,
    storage: ExporterStorage<S>,
    startup_destinations: HashSet<DestinationId>,
    current_committee_destinations: HashSet<DestinationId>,
    // JoinSet to monitor all exporter tasks and detect failures immediately
    pub(super) join_set: JoinSet<(DestinationId, anyhow::Result<()>)>,
    // AbortHandles to selectively abort tasks when needed (e.g., old committee members)
    abort_handles: HashMap<DestinationId, AbortHandle>,
}

impl<F, S> ExportersTracker<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    pub(super) fn new(
        node_options: NodeOptions,
        work_queue_size: usize,
        shutdown_signal: F,
        storage: ExporterStorage<S>,
        startup_destinations: Vec<Destination>,
    ) -> Self {
        let exporters_builder =
            ExporterBuilder::new(node_options, work_queue_size, shutdown_signal);
        Self {
            exporters_builder,
            storage,
            startup_destinations: startup_destinations
                .into_iter()
                .map(|destination| destination.id())
                .collect(),
            current_committee_destinations: HashSet::new(),
            join_set: JoinSet::new(),
            abort_handles: HashMap::new(),
        }
    }

    pub(super) fn start_startup_exporters(&mut self)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        for id in self.startup_destinations.clone() {
            self.spawn(id.clone())
        }
    }

    pub(super) fn start_committee_exporters(&mut self, destination_ids: Vec<DestinationId>)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        for destination in destination_ids {
            if !self.startup_destinations.contains(&destination)
                && !self.current_committee_destinations.contains(&destination)
            {
                self.current_committee_destinations
                    .insert(destination.clone());
                tracing::trace!(id=?destination, "starting committee exporter");
                self.spawn(destination);
            } else {
                tracing::trace!(id=?destination, "skipping already running committee exporter");
            }
        }
    }

    /// Shuts down block exporters for destinations that are not in the new committee.
    pub(super) fn shutdown_old_committee(&mut self, new_committee: Vec<DestinationId>) {
        let new_committee_set: HashSet<_> = new_committee.iter().cloned().collect();
        // Shutdown the old committee members that are not in the new committee.
        for id in self
            .current_committee_destinations
            .difference(&new_committee_set)
        {
            tracing::trace!(id=?id, "shutting down old committee member");
            if let Some(abort_handle) = self.abort_handles.remove(id) {
                abort_handle.abort();
            }
        }
    }

    pub(super) async fn join_all(mut self) {
        while let Some(result) = self.join_set.join_next().await {
            match result {
                Ok((id, Ok(()))) => {
                    tracing::info!(destination_id=?id, "Exporter task completed successfully");
                }
                Ok((id, Err(e))) => {
                    tracing::error!(destination_id=?id, error=?e, "Exporter task failed");
                }
                Err(join_err) => {
                    tracing::error!(error=?join_err, "Task panicked or was cancelled");
                }
            }
        }
    }

    fn spawn(&mut self, id: DestinationId) {
        let exporter_builder = &self.exporters_builder;
        let storage = self.storage.clone().expect("Failed to clone storage");
        let join_handle = exporter_builder.spawn(id.clone(), storage);

        // Spawn task into JoinSet with ID for tracking and store abort handle
        let id_for_task = id.clone();
        let abort_handle = self.join_set.spawn(async move {
            let result = match join_handle.await {
                Ok(r) => r, // Task completed, propagate its Result
                Err(join_err) => Err(anyhow::anyhow!("Task panicked: {}", join_err)),
            };
            (id_for_task, result)
        });

        self.abort_handles.insert(id, abort_handle);
    }
}

/// All the data required by a thread to spawn different tasks
/// on its runtime, join the thread, handle the committees etc.
pub(super) struct ExporterBuilder<F> {
    options: NodeOptions,
    work_queue_size: usize,
    node_provider: Arc<GrpcNodeProvider>,
    shutdown_signal: F,
}

impl<F> ExporterBuilder<F>
where
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    pub(super) fn new(options: NodeOptions, work_queue_size: usize, shutdown_signal: F) -> Self {
        let node_provider = GrpcNodeProvider::new(options);
        let arced_node_provider = Arc::new(node_provider);

        Self {
            options,
            shutdown_signal,
            work_queue_size,
            node_provider: arced_node_provider,
        }
    }

    pub(super) fn spawn<S>(
        &self,
        id: DestinationId,
        storage: ExporterStorage<S>,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>>
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        match id.kind() {
            DestinationKind::Indexer => {
                let exporter_task =
                    super::IndexerExporter::new(id.clone(), self.work_queue_size, self.options);

                tokio::task::spawn(
                    exporter_task.run_with_shutdown(self.shutdown_signal.clone(), storage),
                )
            }

            DestinationKind::Validator => {
                let exporter_task = super::ValidatorExporter::new(
                    id.clone(),
                    self.node_provider.clone(),
                    self.work_queue_size,
                );

                tokio::task::spawn(
                    exporter_task.run_with_shutdown(self.shutdown_signal.clone(), storage),
                )
            }

            DestinationKind::Logging => {
                let path = Path::new(id.address());
                let exporter_task = LoggingExporter::new(path);
                tokio::task::spawn(
                    exporter_task.run_with_shutdown(self.shutdown_signal.clone(), storage),
                )
            }
        }
    }
}
