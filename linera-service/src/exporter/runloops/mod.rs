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
    runtime::Handle,
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    task::{JoinError, JoinSet},
};
use validator_exporter::Exporter as ValidatorExporter;

use crate::{
    common::{BlockId, ExporterError},
    runloops::validator_exporter::ValidatorDestinationKind,
    storage::{BlockProcessorStorage, ExporterStorage},
};

mod block_processor;
mod indexer;
mod validator_exporter;

#[cfg(test)]
pub use indexer::indexer_api;

#[expect(clippy::type_complexity)]
pub(crate) fn start_block_processor_task<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    max_exporter_threads: usize,
    destination_config: DestinationConfig,
) -> Result<
    (
        UnboundedSender<BlockId>,
        JoinHandle<Result<(), ExporterError>>,
    ),
    ExporterError,
>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (task_sender, queue_front) = unbounded_channel();
    let moved_task_sender = task_sender.clone();
    let handle = thread::spawn(move || {
        start_block_processor(
            storage,
            shutdown_signal,
            limits,
            options,
            block_exporter_id,
            max_exporter_threads,
            moved_task_sender,
            destination_config,
            queue_front,
        )
    });

    Ok((task_sender, handle))
}

#[allow(clippy::too_many_arguments)]
#[tokio::main(flavor = "current_thread")]
async fn start_block_processor<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    max_exporter_threads: usize,
    queue_rear: UnboundedSender<BlockId>,
    destination_config: DestinationConfig,
    queue_front: UnboundedReceiver<BlockId>,
) -> Result<(), ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (block_processor_storage, mut exporter_storage) = BlockProcessorStorage::load(
        storage.clone(),
        block_exporter_id,
        destination_config.destinations.len() as u16,
        limits,
    )
    .await?;

    let pool_handles = start_theadpool(max_exporter_threads);

    let pool_members = pool_handles
        .into_iter()
        .map(|(join_handle, runtime_handle)| {
            PoolMember::new(
                runtime_handle,
                join_handle,
                options,
                exporter_storage.clone().unwrap(),
                limits.work_queue_size.into(),
                shutdown_signal.clone(),
            )
        })
        .collect();
    let mut pool_state = ThreadPoolState::new(pool_members);

    let destination_context = DestinationContext::from_config(destination_config);
    pool_state.start_exporters(destination_context);

    let mut block_processor =
        BlockProcessor::new(pool_state, block_processor_storage, queue_rear, queue_front);

    block_processor
        .run_with_shutdown(shutdown_signal, limits.persistence_period_ms)
        .await?;

    block_processor.pool_state().join_all();

    Ok(())
}

fn start_theadpool(number_of_threads: usize) -> Vec<(JoinHandle<()>, Handle)> {
    let mut handles = Vec::new();

    for _ in 0..number_of_threads {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let runtime_handle = runtime.handle().clone();
        let thread_handle = thread::spawn(move || {
            runtime.block_on(std::future::pending::<()>());
        });

        handles.push((thread_handle, runtime_handle));
    }

    handles
}

enum DestinationContext {
    Committee(usize),
    NonCommittee(Vec<Destination>),
}

impl DestinationContext {
    fn from_config(config: DestinationConfig) -> Self {
        Self::NonCommittee(config.destinations.clone())
    }

    fn from_committee_addresses(addresses: &[String]) -> Self {
        Self::Committee(addresses.len())
    }
}

struct ThreadPoolState<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    next_thread_index: usize,
    next_spawn_id: DestinationId,
    threads: Vec<PoolMember<F, S>>,
    next_committee_spawn_id: DestinationId,
}

impl<F, S> ThreadPoolState<F, S>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    fn new(threads: Vec<PoolMember<F, S>>) -> Self {
        Self {
            threads,
            next_spawn_id: 0,
            next_thread_index: 0,
            next_committee_spawn_id: 0,
        }
    }

    fn spawn(&mut self, destination: Destination) {
        let id = self.next_spawn_id;
        self.next_spawn_id += 1;
        let handle = self.get_pool_handle();
        handle.spawn(id, destination)
    }

    fn spawn_committee(&mut self) {
        let id = self.next_committee_spawn_id;
        self.next_committee_spawn_id += 1;
        let handle = self.get_pool_handle();
        handle.spawn_committee(id);
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

    fn start_exporters(&mut self, destination_context: DestinationContext)
    where
        S: Storage + Clone + Send + Sync + 'static,
        F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
        <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
    {
        match destination_context {
            DestinationContext::Committee(committee_size) => {
                for _ in 0..committee_size {
                    self.spawn_committee()
                }
            }

            DestinationContext::NonCommittee(destinations) => {
                for destination in destinations {
                    self.spawn(destination)
                }
            }
        }
    }

    fn join_all(self) {
        for thread in self.threads {
            thread.join_handle.join().unwrap();
        }
    }
}

struct PoolMember<F, S>
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
    fn new(
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

    fn spawn(&mut self, id: DestinationId, destination: Destination) {
        let handle = match destination.kind {
            DestinationKind::Indexer => {
                let exporter_task = IndexerExporter::new(
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
                let destination = ValidatorDestinationKind::NonCommitteeMember(destination);
                let exporter_task = ValidatorExporter::new(
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

    fn spawn_committee(&mut self, id: DestinationId) {
        let destination = ValidatorDestinationKind::CommitteeMember;
        let exporter_task = ValidatorExporter::new(
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
    use std::{collections::BTreeMap, time::Duration};

    use linera_base::{
        crypto::{AccountPublicKey, Secp256k1PublicKey},
        data_types::{
            Blob, BlobContent, ChainDescription, ChainOrigin, Epoch, InitialChainConfig, Round,
            Timestamp,
        },
        port::get_free_port,
    };
    use linera_chain::{
        data_types::BlockExecutionOutcome,
        test::{make_first_block, BlockTestExt},
        types::{ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_client::config::{Destination, DestinationConfig, LimitsConfig};
    use linera_execution::{
        committee::{Committee, ValidatorState},
        system::AdminOperation,
        Operation, ResourceControlPolicy, SystemOperation,
    };
    use linera_rpc::{config::TlsConfig, NodeOptions};
    use linera_service::cli_wrappers::local_net::LocalNet;
    use linera_storage::{DbStorage, Storage};
    use linera_views::memory::MemoryStore;
    use test_case::test_case;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::start_block_processor_task;
    use crate::{
        common::{BlockId, CanonicalBlock},
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

        let (notifier, _handle) = start_block_processor_task(
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
            1,
            DestinationConfig {
                committee_destination: false,
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

        let (notifier, _handle) = start_block_processor_task(
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
                committee_destination: false,
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
        let (_, _, destination_states, _) =
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
            DestinationConfig {
                destinations,
                committee_destination: false,
            },
        )?;

        sleep(Duration::from_secs(4)).await;

        child.cancel();

        let (_, _, destination_states, _) =
            BlockExporterStateView::initiate(context.clone(), 4).await?;
        for i in 0..4 {
            assert_eq!(destination_states.load_state(i), 2);
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_committee_destination() -> anyhow::Result<()> {
        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let cancellation_token = CancellationToken::new();
        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());

        let (notifier, _handle) = start_block_processor_task(
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
                committee_destination: true,
                destinations: vec![],
            },
        )?;

        let mut destinations = Vec::new();
        let dummy_validator = spawn_dummy_validator(&mut destinations, &cancellation_token).await?;
        let destination = destinations.pop().expect("manually done");
        let validator_state = ValidatorState {
            network_address: destination.address(),
            votes: 0,
            account_public_key: AccountPublicKey::test_key(0),
        };
        let mut validators = BTreeMap::new();
        validators.insert(Secp256k1PublicKey::test_key(0), validator_state);
        let committee = Committee::new(validators, ResourceControlPolicy::testnet());

        let chain_description = ChainDescription::new(
            ChainOrigin::Root(0),
            InitialChainConfig {
                ownership: Default::default(),
                epoch: Default::default(),
                balance: Default::default(),
                application_permissions: Default::default(),
                active_epochs: Default::default(),
            },
            Timestamp::now(),
        );

        let chain_id = chain_description.id();
        let chain_blob = Blob::new_chain_description(&chain_description);

        let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
        let proposed_block = make_first_block(chain_id).with_operation(Operation::System(
            Box::new(SystemOperation::Admin(AdminOperation::CreateCommittee {
                epoch: Epoch::ZERO,
                blob_hash: committee_blob.id().hash,
            })),
        ));
        let blobs = vec![chain_blob, committee_blob];
        let block = BlockExecutionOutcome {
            blobs: vec![blobs.clone()],
            ..Default::default()
        }
        .with(proposed_block);

        let confirmed_block = ConfirmedBlock::new(block);
        let notification = BlockId::from_confirmed_block(&confirmed_block);
        let certificate = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);
        storage
            .write_blobs_and_certificate(blobs.as_ref(), &certificate)
            .await?;

        notifier.send(notification)?;

        sleep(Duration::from_secs(4)).await;

        assert!(dummy_validator.state.contains(&notification.hash));

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
