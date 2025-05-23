// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    future::{Future, IntoFuture},
    sync::Arc,
    thread,
};

use block_processor::BlockProcessor;
use indexer::indexer_exporter::Exporter as IndexerExporter;
use linera_client::config::{
    Destination, DestinationConfig, DestinationId, DestinationKind, LimitsConfig, WalrusConfig,
    WalrusStoreConfig,
};
use linera_rpc::{grpc::GrpcNodeProvider, NodeOptions};
use linera_storage::Storage;
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::JoinSet,
};
use validator_exporter::Exporter as ValidatorExporter;
use walrus::walrus_exporter::Exporter as WalrusExporter;

use crate::{
    common::{BlockId, ExporterError},
    storage::{BlockProcessorStorage, ExporterStorage},
};

mod block_processor;
mod indexer;
mod validator_exporter;
mod walrus;

#[cfg(test)]
pub use indexer::indexer_api;

fn start_exporters<S, F>(
    signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    clients_per_thread: usize,
    mut storage: ExporterStorage<S>,
    destination_config: DestinationConfig,
) -> Result<(), ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let number_of_threads = destination_config
        .destinations
        .len()
        .saturating_add(1)
        .div_ceil(clients_per_thread);
    let _threadpool = {
        let mut pool = Vec::new();
        for n in 0..number_of_threads {
            let moved_signal = signal.clone();
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

            let walrus_config = if 0 == n {
                destination_config.walrus.clone()
            } else {
                None
            };

            let handle = thread::spawn(move || {
                start_exporter_tasks(
                    moved_signal,
                    options,
                    work_queue_size,
                    walrus_config,
                    moved_storage,
                    destinations,
                )
            });

            pool.push(handle);
        }

        pool
    };

    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn start_exporter_tasks<S, F>(
    signal: F,
    options: NodeOptions,
    work_queue_size: usize,
    walrus_config: Option<WalrusConfig<WalrusStoreConfig>>,
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
            signal.clone(),
            id + 1,
            options,
            work_queue_size,
            destination,
            storage.clone().unwrap(),
            arced_node_provider.clone(),
        );
    }

    if let Some(config) = walrus_config {
        let walrus_exporter =
            WalrusExporter::new(0, work_queue_size, storage.clone().unwrap(), config);
        set.spawn(walrus_exporter.run_with_shutdown(signal));
    }

    while let Some(res) = set.join_next().await {
        res.unwrap().unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
#[tokio::main(flavor = "current_thread")]
async fn start_block_processor<S, F>(
    signal: F,
    storage: S,
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
        destination_config.destinations.len() as u16 + 1,
        limits,
    )
    .await?;

    let mut block_processor = BlockProcessor::new(block_processor_storage, queue_rear, queue_front);

    start_exporters(
        signal.clone(),
        options,
        limits.work_queue_size.into(),
        clients_per_thread,
        exporter_storage,
        destination_config,
    )
    .unwrap();

    block_processor
        .run_with_shutdown(signal, limits.persistence_period)
        .await
        .unwrap();

    Ok(())
}

pub(crate) fn start_runloops<S, F>(
    signal: F,
    storage: S,
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
            signal,
            storage,
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
    use std::{sync::atomic::Ordering, time::Duration};

    use linera_base::port::get_free_port;
    use linera_client::config::{
        Destination, DestinationConfig, LimitsConfig, WalrusConfig, WalrusStoreConfig,
    };
    use linera_rpc::{config::TlsConfig, NodeOptions};
    use linera_service::cli_wrappers::local_net::LocalNet;
    use linera_storage::{DbStorage, Storage};
    use linera_views::memory::MemoryStore;
    use tempfile::tempdir;
    use test_case::test_case;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::start_runloops;
    use crate::{
        common::CanonicalBlock,
        state::BlockExporterStateView,
        test_utils::{
            init_walrus_test_enviroment, make_simple_state_with_blobs, DummyIndexer,
            DummyValidator, TestDestination,
        },
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

        let notifier = start_runloops(
            signal,
            storage,
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
                walrus: None,
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

        let notifier = start_runloops(
            signal,
            storage.clone(),
            LimitsConfig {
                persistence_period: 3,
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
                walrus: None,
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
            let state = destination_states.load_state(i + 1);
            if i % 2 == 0 {
                assert_eq!(state, 2);
            } else {
                assert_eq!(state, 0);
            }
        }

        faulty_indexer.fault_guard().store(false, Ordering::Release);
        faulty_validator
            .fault_guard()
            .store(false, Ordering::Release);

        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());

        // restart
        let _notifier = start_runloops(
            signal,
            storage.clone(),
            LimitsConfig {
                persistence_period: 3,
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
                destinations,
                walrus: None,
            },
        )?;

        sleep(Duration::from_secs(4)).await;

        child.cancel();

        let (_, _, destination_states) =
            BlockExporterStateView::initiate(context.clone(), 4).await?;
        for i in 1..5 {
            assert_eq!(destination_states.load_state(i), 2);
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_walrus_destination() -> anyhow::Result<()> {
        let cancellation_token = CancellationToken::new();
        let signal = ExporterCancellationSignal::new(cancellation_token.clone());
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;

        init_walrus_test_enviroment();

        let path = tempdir()?;
        let config = WalrusConfig {
            log: path.path().to_path_buf(),
            client_config: path.path().to_path_buf(),
            wallet_config: path.path().to_path_buf(),
            context: None,
            behaviour: WalrusStoreConfig::default(),
        };

        let notifier = start_runloops(
            signal,
            storage.clone(),
            LimitsConfig {
                persistence_period: 2,
                ..Default::default()
            },
            NodeOptions {
                send_timeout: Duration::from_millis(4000),
                recv_timeout: Duration::from_millis(4000),
                retry_delay: Duration::from_millis(1000),
                max_retries: 10,
            },
            0,
            10,
            DestinationConfig {
                walrus: Some(config),
                destinations: vec![],
            },
        )?;

        let (notification, _state) = make_simple_state_with_blobs(&storage).await;

        assert!(
            notifier.send(notification).is_ok(),
            "notifier should work as long as there exists a receiver to receive notifications"
        );

        // walrus client takes much time to boot up
        // hopefully it does
        tokio::time::sleep(Duration::from_secs(10)).await;

        // we wait till walrus processes everything
        loop {
            let context = storage.block_exporter_context(0).await?;
            let (_, _, destination_states) =
                BlockExporterStateView::initiate(context.clone(), 1).await?;
            let state = destination_states.load_state(0);

            if state == 2 {
                break;
            }

            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        cancellation_token.cancel();

        // just a little more till everything persists
        tokio::time::sleep(Duration::from_secs(3)).await;
        let bytes = tokio::fs::read(path.path().join("current.walrus_log")).await?;
        let string = String::from_utf8(bytes)?;
        assert!(string.contains("successfully stored blob with id"));

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
        destination.fault_guard().store(true, Ordering::Release);
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
        destination.fault_guard().store(true, Ordering::Release);
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
