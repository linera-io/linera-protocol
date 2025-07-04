// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::{Future, IntoFuture},
    sync::Arc,
    thread::JoinHandle,
};

use linera_client::config::{Destination, DestinationId, DestinationKind};
use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_storage::Storage;
use tokio::{
    runtime::Handle,
    task::{JoinError, JoinSet},
};

use crate::storage::ExporterStorage;

/// This type manages tasks like spawning different exporters on the different
/// threads, discarding the committees and joining every thread properly at the
/// end for graceful shutdown of the process.
pub(super) struct ThreadPoolState<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    next_thread_index: usize,
    next_spawn_id: DestinationId,
    threads: Vec<PoolMember<F, S>>,
    next_committee_spawn_id: DestinationId,
    startup_destinations: Vec<Destination>,
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
            next_spawn_id: 0,
            startup_destinations,
            next_thread_index: 0,
            next_committee_spawn_id: 0,
        }
    }

    pub(super) fn start_startup_exporters(&mut self)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        for destination in self.startup_destinations.clone() {
            self.spawn(destination)
        }
    }

    pub(super) fn start_committee_exporters(&mut self, committee_addresses: &[String])
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        let startup_addresses = self
            .startup_destinations
            .iter()
            .map(|destination| destination.address())
            .collect::<Vec<String>>();
        for address in committee_addresses {
            if !startup_addresses.contains(address) {
                self.spawn_committee();
            }
        }
    }

    pub(super) async fn shutdown_current_committee(&mut self) {
        for thread in &mut self.threads {
            if let Some(set) = &mut thread.committee_task_handles {
                set.shutdown().await;
            }
        }
    }

    pub(super) fn join_all(self) {
        for thread in self.threads {
            thread.join_handle.join().unwrap();
        }
    }

    fn spawn(&mut self, destination: Destination) {
        let id = self.next_spawn_id();
        let handle = self.get_pool_handle();
        handle.spawn(id, destination)
    }

    fn spawn_committee(&mut self) {
        let id = self.next_committee_spawn_id();
        self.next_committee_spawn_id += 1;
        let handle = self.get_pool_handle();
        handle.spawn_committee(id);
    }

    fn next_spawn_id(&mut self) -> u16 {
        let id = self.next_spawn_id;
        self.next_spawn_id += 1;
        id
    }

    fn next_committee_spawn_id(&mut self) -> u16 {
        let id = self.next_committee_spawn_id;
        self.next_committee_spawn_id += 1;
        id
    }

    fn get_pool_handle(&mut self) -> &mut PoolMember<F, S> {
        let number_of_threads = self.threads.len();
        let handle = self
            .threads
            .get_mut(self.next_thread_index)
            .expect("handle_index is manually checked below");

        self.next_thread_index += 1;
        if self.next_thread_index == number_of_threads {
            self.next_thread_index = 0;
        }

        handle
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
    runtime_handle: Handle,
    task_handles: JoinSet<Result<anyhow::Result<()>, JoinError>>,
    storage: ExporterStorage<S>,
    join_handle: JoinHandle<()>,
    node_provider: Arc<GrpcNodeProvider>,
    committee_task_handles: Option<JoinSet<Result<anyhow::Result<()>, JoinError>>>,
}

impl<F, S> PoolMember<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    pub(super) fn new(
        runtime_handle: Handle,
        join_handle: JoinHandle<()>,
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
            join_handle,
            runtime_handle,
            shutdown_signal,
            work_queue_size,
            committee_task_handles: None,
            task_handles: JoinSet::new(),
            node_provider: arced_node_provider,
        }
    }

    pub(super) fn spawn(&mut self, id: DestinationId, destination: Destination) {
        let handle = match destination.kind {
            DestinationKind::Indexer => {
                let exporter_task = super::IndexerExporter::new(
                    self.options,
                    self.work_queue_size,
                    self.storage.clone().unwrap(),
                    id,
                    destination,
                );

                self.runtime_handle
                    .spawn(exporter_task.run_with_shutdown(self.shutdown_signal.clone()))
            }

            DestinationKind::Validator => {
                let destination = super::ValidatorDestinationKind::NonCommitteeMember(destination);
                let exporter_task = super::ValidatorExporter::new(
                    self.node_provider.clone(),
                    id,
                    self.storage.clone().unwrap(),
                    destination,
                    self.work_queue_size,
                );

                self.runtime_handle
                    .spawn(exporter_task.run_with_shutdown(self.shutdown_signal.clone()))
            }
        };

        let _abort_handle = self.task_handles.spawn(handle);
    }

    pub(super) fn spawn_committee(&mut self, id: DestinationId) {
        let destination = super::ValidatorDestinationKind::CommitteeMember;
        let exporter_task = super::ValidatorExporter::new(
            self.node_provider.clone(),
            id,
            self.storage.clone().unwrap(),
            destination,
            self.work_queue_size,
        );
        let handle = self
            .runtime_handle
            .spawn(exporter_task.run_with_shutdown(self.shutdown_signal.clone()));
        let _abort_handle = self
            .committee_task_handles
            .get_or_insert_default()
            .spawn(handle);
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use linera_client::config::LimitsConfig;
    use linera_rpc::NodeOptions;
    use linera_storage::DbStorage;
    use linera_views::memory::MemoryStore;
    use tokio_util::sync::CancellationToken;

    use crate::{
        common::ExporterCancellationSignal,
        runloops::{start_threadpool, PoolMember, ThreadPoolState},
        storage::BlockProcessorStorage,
    };

    // tests the spawnd_ids and thread_ids etc.
    // are working correctly.
    #[test_log::test(tokio::test)]
    async fn test_threadpool_state_id() -> anyhow::Result<()> {
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let cancellation_token = CancellationToken::new();
        let shutdown_signal = ExporterCancellationSignal::new(cancellation_token.clone());

        let (_block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, 0, LimitsConfig::default()).await?;

        let number_of_threads = 2;
        let pool_handles = start_threadpool(number_of_threads);
        let pool_members = pool_handles
            .into_iter()
            .map(|(join_handle, runtime_handle)| {
                PoolMember::new(
                    runtime_handle,
                    join_handle,
                    NodeOptions {
                        send_timeout: Duration::from_millis(1000),
                        recv_timeout: Duration::from_millis(1000),
                        retry_delay: Duration::from_millis(1000),
                        max_retries: 4,
                    },
                    exporter_storage.clone().unwrap(),
                    2,
                    shutdown_signal.clone(),
                )
            })
            .collect();

        let mut pool_state = ThreadPoolState::new(pool_members, vec![]);

        // start from zero and the on every call
        // `next_spawn_id()` and `next_committee_spawn_id()`
        // increments the id spawn_id by 1.

        assert_eq!(pool_state.next_spawn_id, 0);
        assert_eq!(pool_state.next_spawn_id(), 0);
        assert_eq!(pool_state.next_spawn_id(), 1);
        assert_eq!(pool_state.next_spawn_id(), 2);
        assert_eq!(pool_state.next_spawn_id(), 3);

        assert_eq!(pool_state.next_committee_spawn_id, 0);
        assert_eq!(pool_state.next_committee_spawn_id(), 0);
        assert_eq!(pool_state.next_committee_spawn_id(), 1);
        assert_eq!(pool_state.next_committee_spawn_id(), 2);
        assert_eq!(pool_state.next_committee_spawn_id(), 3);

        // `get_pool_handle()` increments and round backs to the first thread
        // if it readches the maximum number of threads specified.

        assert_eq!(pool_state.next_thread_index, 0);

        let _ = pool_state.get_pool_handle();
        assert_eq!(pool_state.next_thread_index, 1);

        let _ = pool_state.get_pool_handle();
        assert_eq!(pool_state.next_thread_index, 0);

        Ok(())
    }
}
