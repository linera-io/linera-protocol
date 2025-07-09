// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    future::{Future, IntoFuture},
    sync::Arc,
    time::Duration,
};

use alloy_primitives::map::HashMap;
use linera_client::config::{Destination, DestinationId, DestinationKind};
use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_storage::Storage;
use tokio::{
    runtime::Runtime,
    task::{AbortHandle, JoinError, JoinSet},
};

use crate::storage::ExporterStorage;

/// This type manages tasks like spawning different exporters on the different
/// threads, discarding the committees and joining every thread properly at the
/// end for graceful shutdown of the process.
pub(super) struct ThreadPoolState<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    threads: Vec<PoolMember<F, S>>, // This shouldn't need to be a Vec
    startup_destinations: HashMap<DestinationId, Destination>,
    current_committee_destinations: HashSet<DestinationId>,
}

impl<F, S> ThreadPoolState<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    pub(super) fn new(
        threads: Vec<PoolMember<F, S>>,
        startup_destinations: Vec<Destination>,
    ) -> Self {
        Self {
            threads,
            startup_destinations: startup_destinations
                .into_iter()
                .map(|destination| (destination.id(), destination))
                .collect(),
            current_committee_destinations: HashSet::new(),
        }
    }

    pub(super) fn start_startup_exporters(&mut self)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        for (id, _destination) in self.startup_destinations.clone().iter() {
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
            if !self.startup_destinations.contains_key(&destination)
                && !self.current_committee_destinations.contains(&destination)
            {
                self.current_committee_destinations
                    .insert(destination.clone());
                tracing::trace!(id=?destination, "starting committee exporter");
                self.spawn(destination);
            }
        }
    }

    /// Shuts down block exporters for destinations that are not in the new committee.
    pub(super) async fn shutdown_old_committee(&mut self, new_committee: Vec<DestinationId>) {
        // Shutdown the old committee members that are not in the new committee.
        for id in self
            .current_committee_destinations
            .difference(&new_committee.iter().cloned().collect())
        {
            tracing::trace!(id=?id, "shutting down old committee member");
            if let Some(abort_handle) = self.threads[0].abort_handles.remove(id) {
                abort_handle.abort();
            }
        }
    }

    pub(super) fn join_all(self) {
        for thread in self.threads {
            // Wait for all tasks to finish.
            let _ = thread
                .runtime_handle
                .block_on(thread.task_handles.join_all());

            // Shutdown the runtime.
            thread
                .runtime_handle
                .shutdown_timeout(Duration::from_secs(1));
        }
    }

    fn spawn(&mut self, id: DestinationId) {
        let handle = self.get_pool_handle();
        handle.spawn(id)
    }

    fn get_pool_handle(&mut self) -> &mut PoolMember<F, S> {
        &mut self.threads[0]
    }
}

/// All the data required by a thread to spawn different tasks
/// on its runtime, join the thread, handle the committees etc.
pub(super) struct PoolMember<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    shutdown_signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    runtime_handle: Runtime,
    task_handles: JoinSet<Result<anyhow::Result<()>, JoinError>>,
    abort_handles: HashMap<DestinationId, AbortHandle>,
    storage: ExporterStorage<S>,
    node_provider: Arc<GrpcNodeProvider>,
}

impl<F, S> PoolMember<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    pub(super) fn new(
        runtime_handle: Runtime,
        options: NodeOptions,
        storage: ExporterStorage<S>,
        work_queue_size: usize,
        shutdown_signal: F,
    ) -> Self {
        let node_provider = GrpcNodeProvider::new(options);
        let arced_node_provider = Arc::new(node_provider);

        Self {
            storage,
            options,
            runtime_handle,
            shutdown_signal,
            work_queue_size,
            abort_handles: HashMap::new(),
            task_handles: JoinSet::new(),
            node_provider: arced_node_provider,
        }
    }

    pub(super) fn spawn(&mut self, id: DestinationId) {
        let handle = match id.kind() {
            DestinationKind::Indexer => {
                let exporter_task = super::IndexerExporter::new(
                    self.options,
                    self.work_queue_size,
                    self.storage.clone().unwrap(),
                    id.clone(),
                );

                self.runtime_handle
                    .spawn(exporter_task.run_with_shutdown(self.shutdown_signal.clone()))
            }

            DestinationKind::Validator => {
                let exporter_task = super::ValidatorExporter::new(
                    self.node_provider.clone(),
                    id.clone(),
                    self.storage.clone().unwrap(),
                    self.work_queue_size,
                );

                self.runtime_handle
                    .spawn(exporter_task.run_with_shutdown(self.shutdown_signal.clone()))
            }
        };

        self.abort_handles
            .insert(id, self.task_handles.spawn(handle));
    }
}
