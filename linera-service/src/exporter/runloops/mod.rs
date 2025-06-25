// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::{Future, IntoFuture},
    sync::Arc,
    thread::{self, JoinHandle},
};

use block_processor::BlockProcessor;
use indexer::indexer_exporter::Exporter as IndexerExporter;
use linera_client::config::{
    Destination, DestinationConfig, DestinationId, DestinationKind, LimitsConfig,
};
use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_storage::Storage;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};
use validator_exporter::Exporter as ValidatorExporter;

use crate::{
    common::{BlockId, ExporterError},
    storage::{BlockProcessorStorage, ExporterStorage},
};

mod block_processor;
mod indexer;
mod validator_exporter;

#[cfg(test)]
pub use indexer::indexer_api;

pub(crate) fn start_block_processor_task<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    clients_per_thread: usize,
    destination_config: DestinationConfig,
) -> Result<UnboundedSender<BlockId>, ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (task_sender, queue_front) = unbounded_channel();
    let moved_task_sender = task_sender.clone();
    let _handle = thread::spawn(move || {
        start_block_processor(
            storage,
            shutdown_signal,
            limits,
            options,
            block_exporter_id,
            clients_per_thread,
            moved_task_sender,
            destination_config,
            queue_front,
        )
    });

    Ok(task_sender)
}

fn start_exporters<S, F>(
    shutdown_signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    clients_per_thread: usize,
    mut storage: ExporterStorage<S>,
    destination_config: DestinationConfig,
) -> Result<Vec<JoinHandle<()>>, ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let number_of_threads = destination_config
        .destinations
        .len()
        .div_ceil(clients_per_thread);
    let threadpool = {
        let mut pool = Vec::new();
        for n in 0..number_of_threads {
            let moved_signal = shutdown_signal.clone();
            let moved_storage = storage.clone()?;
            let destinations = destination_config
                .destinations
                .iter()
                .enumerate()
                .filter_map(|(i, dest)| {
                    if i % number_of_threads == n {
                        Some((i as u16, dest.clone()))
                    } else {
                        None
                    }
                })
                .collect();

            let handle = thread::spawn(move || {
                start_exporter_tasks(
                    moved_signal,
                    options,
                    work_queue_size,
                    moved_storage,
                    destinations,
                )
            });

            pool.push(handle);
        }

        pool
    };

    Ok(threadpool)
}

#[tokio::main(flavor = "current_thread")]
async fn start_exporter_tasks<S, F>(
    shutdown_signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    mut storage: ExporterStorage<S>,
    destinations: Vec<(DestinationId, Destination)>,
) where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let node_provider = GrpcNodeProvider::new(options);
    let arced_node_provider = Arc::new(node_provider);
    let mut set = JoinSet::new();
    for (id, destination) in destinations {
        spawn_exporter_task_on_set(
            &mut set,
            shutdown_signal.clone(),
            id,
            options,
            work_queue_size,
            destination,
            storage.clone().unwrap(),
            arced_node_provider.clone(),
        );
    }

    while let Some(res) = set.join_next().await {
        let _res = res.unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
#[tokio::main(flavor = "current_thread")]
async fn start_block_processor<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    clients_per_thread: usize,
    queue_rear: UnboundedSender<BlockId>,
    destination_config: DestinationConfig,
    queue_front: UnboundedReceiver<BlockId>,
) -> Result<(), ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (block_processor_storage, exporter_storage) = BlockProcessorStorage::load(
        storage.clone(),
        block_exporter_id,
        destination_config.destinations.len() as u16,
        limits,
    )
    .await?;

    let mut block_processor = BlockProcessor::new(block_processor_storage, queue_rear, queue_front);

    let _pool = start_exporters(
        shutdown_signal.clone(),
        options,
        limits.work_queue_size.into(),
        clients_per_thread,
        exporter_storage,
        destination_config,
    )
    .unwrap();

    block_processor
        .run_with_shutdown(shutdown_signal, limits.persistence_period_ms)
        .await
        .unwrap();

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn spawn_exporter_task_on_set<F, S>(
    set: &mut JoinSet<Result<(), anyhow::Error>>,
    signal: F,
    id: DestinationId,
    options: NodeOptions,
    work_queue_size: usize,
    destination: Destination,
    storage: ExporterStorage<S>,
    node_provider: Arc<GrpcNodeProvider>,
) where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    match destination.kind {
        DestinationKind::Indexer => {
            let exporter_task =
                IndexerExporter::new(options, work_queue_size, storage, id, destination);

            let _handle = set.spawn(exporter_task.run_with_shutdown(signal));
        }

        DestinationKind::Validator => {
            let exporter_task =
                ValidatorExporter::new(node_provider, id, storage, destination, work_queue_size);

            let _handle = set.spawn(exporter_task.run_with_shutdown(signal));
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use linera_base::port::get_free_port;
    use linera_client::config::{Destination, DestinationConfig, LimitsConfig};
    use linera_rpc::{config::TlsConfig, NodeOptions};
    use linera_service::cli_wrappers::local_net::LocalNet;
    use linera_storage::{DbStorage, Storage};
    use linera_views::memory::MemoryStore;
    use test_case::test_case;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::start_block_processor_task;
    use crate::{
        common::CanonicalBlock,
        state::BlockExporterStateView,
        test_utils::{make_simple_state_with_blobs, DummyIndexer, DummyValidator, TestDestination},
        ExporterCancellationSignal,
    };

    #[test_case(DummyIndexer::default())]
    #[test_case(DummyValidator::default())]
    #[test_log::test(tokio::test)]
    async fn test_destinations<T>(destination: T) -> Result<(), anyhow::Error>
    where
        T: TestDestination + Clone + Send + 'static,
    {
        let port = get_free_port().await?;
        let cancellation_token = CancellationToken::new();
        tokio::spawn(destination.clone().start(port, cancellation_token.clone()));
        LocalNet::ensure_grpc_server_has_started("test server", port as usize, "http").await?;

        let signal = ExporterCancellationSignal::new(cancellation_token.clone());
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let destination_address = Destination {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
            kind: destination.kind(),
        };

        // make some blocks
        let (notification, state) = make_simple_state_with_blobs(&storage).await;

        let notifier = start_block_processor_task(
            storage,
            signal,
            LimitsConfig::default(),
            NodeOptions {
                send_timeout: Duration::from_millis(4000),
                recv_timeout: Duration::from_millis(4000),
                retry_delay: Duration::from_millis(1000),
                max_retries: 10,
            },
            0,
            10,
            DestinationConfig {
                destinations: vec![destination_address],
            },
        )?;

        assert!(
            notifier.send(notification).is_ok(),
            "notifier should work as long as there exists a receiver to receive notifications"
        );

        sleep(Duration::from_secs(4)).await;

        for CanonicalBlock { blobs, block_hash } in state {
            assert!(destination.state().contains(&block_hash));
            for blob in blobs {
                assert!(destination.blobs().contains(&blob));
            }
        }

        cancellation_token.cancel();

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_restart_persistence_and_faulty_destination_restart() -> Result<(), anyhow::Error>
    {
        let mut destinations = Vec::new();
        let cancellation_token = CancellationToken::new();
        let _dummy_indexer = spawn_dummy_indexer(&mut destinations, &cancellation_token).await?;
        let faulty_indexer = spawn_faulty_indexer(&mut destinations, &cancellation_token).await?;
        let _dummy_validator =
            spawn_dummy_validator(&mut destinations, &cancellation_token).await?;
        let faulty_validator =
            spawn_faulty_validator(&mut destinations, &cancellation_token).await?;

        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;

        let (notification, _state) = make_simple_state_with_blobs(&storage).await;

        let notifier = start_block_processor_task(
            storage.clone(),
            signal,
            LimitsConfig {
                persistence_period_ms: 3000,
                ..Default::default()
            },
            NodeOptions {
                send_timeout: Duration::from_millis(4000),
                recv_timeout: Duration::from_millis(4000),
                retry_delay: Duration::from_millis(1000),
                max_retries: 10,
            },
            0,
            1,
            DestinationConfig {
                destinations: destinations.clone(),
            },
        )?;

        assert!(
            notifier.send(notification).is_ok(),
            "notifier should work as long as there exists a receiver to receive notifications"
        );

        sleep(Duration::from_secs(4)).await;

        child.cancel();

        let context = storage.block_exporter_context(0).await?;
        let (_, _, destination_states) =
            BlockExporterStateView::initiate(context.clone(), 4).await?;
        for i in 0..4 {
            let state = destination_states.load_state(i);
            if i % 2 == 0 {
                assert_eq!(state, 2);
            } else {
                assert_eq!(state, 0);
            }
        }

        faulty_indexer.unset_faulty();
        faulty_validator.unset_faulty();

        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());

        // restart
        let _notifier = start_block_processor_task(
            storage.clone(),
            signal,
            LimitsConfig {
                persistence_period_ms: 3000,
                ..Default::default()
            },
            NodeOptions {
                send_timeout: Duration::from_millis(4000),
                recv_timeout: Duration::from_millis(4000),
                retry_delay: Duration::from_millis(1000),
                max_retries: 10,
            },
            0,
            1,
            DestinationConfig { destinations },
        )?;

        sleep(Duration::from_secs(4)).await;

        child.cancel();

        let (_, _, destination_states) =
            BlockExporterStateView::initiate(context.clone(), 4).await?;
        for i in 0..4 {
            assert_eq!(destination_states.load_state(i), 2);
        }

        Ok(())
    }

    async fn spawn_dummy_indexer(
        destinations: &mut Vec<Destination>,
        token: &CancellationToken,
    ) -> anyhow::Result<DummyIndexer> {
        let port = get_free_port().await?;
        let destination = DummyIndexer::default();
        tokio::spawn(destination.clone().start(port, token.clone()));
        LocalNet::ensure_grpc_server_has_started("dummy indexer", port as usize, "http").await?;
        let destination_address = Destination {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
            kind: destination.kind(),
        };

        destinations.push(destination_address);
        Ok(destination)
    }

    async fn spawn_dummy_validator(
        destinations: &mut Vec<Destination>,
        token: &CancellationToken,
    ) -> anyhow::Result<DummyValidator> {
        let port = get_free_port().await?;
        let destination = DummyValidator::default();
        tokio::spawn(destination.clone().start(port, token.clone()));
        LocalNet::ensure_grpc_server_has_started("dummy validator", port as usize, "http").await?;
        let destination_address = Destination {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
            kind: destination.kind(),
        };

        destinations.push(destination_address);
        Ok(destination)
    }

    async fn spawn_faulty_indexer(
        destinations: &mut Vec<Destination>,
        token: &CancellationToken,
    ) -> anyhow::Result<DummyIndexer> {
        let port = get_free_port().await?;
        let destination = DummyIndexer::default();
        destination.set_faulty();
        tokio::spawn(destination.clone().start(port, token.clone()));
        LocalNet::ensure_grpc_server_has_started("faulty indexer", port as usize, "http").await?;
        let destination_address = Destination {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
            kind: destination.kind(),
        };

        destinations.push(destination_address);
        Ok(destination)
    }

    async fn spawn_faulty_validator(
        destinations: &mut Vec<Destination>,
        token: &CancellationToken,
    ) -> anyhow::Result<DummyValidator> {
        let port = get_free_port().await?;
        let destination = DummyValidator::default();
        destination.set_faulty();
        tokio::spawn(destination.clone().start(port, token.clone()));
        LocalNet::ensure_grpc_server_has_started("falty validator", port as usize, "http").await?;
        let destination_address = Destination {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
            kind: destination.kind(),
        };

        destinations.push(destination_address);
        Ok(destination)
    }
}
