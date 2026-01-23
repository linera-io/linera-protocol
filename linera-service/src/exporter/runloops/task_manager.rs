// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::{HashMap, HashSet},
    future::{Future, IntoFuture},
    sync::Arc,
};

use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_service::config::{Destination, DestinationId, DestinationKind};
use linera_storage::Storage;

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
    // Handles to all the exporter tasks spawned. Allows to join them later and shut down the thread gracefully.
    join_handles: HashMap<DestinationId, tokio::task::JoinHandle<anyhow::Result<()>>>,
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
        current_committee_destinations: HashSet<DestinationId>,
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
            current_committee_destinations,
            join_handles: HashMap::new(),
        }
    }

    pub(super) fn spawn_exporters(&mut self)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        for id in self.startup_destinations.clone() {
            self.spawn(id.clone());
        }

        self.start_committee_exporters(self.current_committee_destinations.clone());
    }

    pub(super) fn start_committee_exporters(&mut self, destination_ids: HashSet<DestinationId>)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        for destination in destination_ids {
            // We treat startup destinations as "MUST" always run
            // so we skip adding them to `current_committee_destinations` as those
            // can be turned off.
            if !self.startup_destinations.contains(&destination)
                && !self.current_committee_destinations.contains(&destination)
            {
                self.current_committee_destinations
                    .insert(destination.clone());
                tracing::info!(id=?destination, "starting committee exporter");
                self.spawn(destination);
            } else {
                tracing::info!(id=?destination, "skipping already running committee exporter");
            }
        }
    }

    /// Shuts down block exporters for destinations that are not in the new committee.
    pub(super) fn shutdown_old_committee(&mut self, new_committee: HashSet<DestinationId>) {
        // Shutdown the old committee members that are not in the new committee.
        for id in self
            .current_committee_destinations
            .difference(&new_committee)
        {
            tracing::info!(id=?id, "shutting down old committee member");
            if let Some(abort_handle) = self.join_handles.remove(id) {
                abort_handle.abort();
            }
        }
    }

    pub(super) async fn join_all(self) {
        for (id, handle) in self.join_handles {
            // Wait for all tasks to finish.
            if let Err(e) = handle.await.unwrap() {
                tracing::error!(id=?id, error=?e, "failed to join task");
            }
        }
    }

    fn spawn(&mut self, id: DestinationId) {
        let exporter_builder = &self.exporters_builder;
        let storage = self.storage.clone().expect("Failed to clone storage");
        if self.join_handles.contains_key(&id) {
            tracing::trace!(id=?id, "exporter already running, skipping spawn");
            return;
        }
        let join_handle = exporter_builder.spawn(id.clone(), storage);
        self.join_handles.insert(id, join_handle);
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
                let exporter_task = LoggingExporter::new(id);
                tokio::task::spawn(
                    exporter_task.run_with_shutdown(self.shutdown_signal.clone(), storage),
                )
            }
        }
    }
}
