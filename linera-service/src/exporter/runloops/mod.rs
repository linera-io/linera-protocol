// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::HashSet,
    future::{Future, IntoFuture},
};

use block_processor::BlockProcessor;
use indexer::indexer_exporter::Exporter as IndexerExporter;
use linera_base::identifiers::BlobId;
use linera_execution::committee::Committee;
use linera_rpc::NodeOptions;
use linera_service::config::{DestinationConfig, DestinationId, LimitsConfig};
use linera_storage::Storage;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use validator_exporter::Exporter as ValidatorExporter;

use crate::{
    common::{BlockId, ExporterError},
    runloops::task_manager::ExportersTracker,
    storage::BlockProcessorStorage,
};

mod block_processor;
mod indexer;
mod logging_exporter;
mod task_manager;
mod validator_exporter;

#[cfg(test)]
pub use indexer::indexer_api;

pub(crate) fn start_block_processor_task<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    destination_config: DestinationConfig,
) -> (
    UnboundedSender<BlockId>,
    std::thread::JoinHandle<Result<(), ExporterError>>,
)
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let (task_sender, queue_front) = unbounded_channel();
    let new_block_queue = NewBlockQueue {
        queue_rear: task_sender.clone(),
        queue_front,
    };
    let handle = std::thread::spawn(move || {
        start_block_processor(
            storage,
            shutdown_signal,
            limits,
            options,
            block_exporter_id,
            new_block_queue,
            destination_config,
        )
    });

    (task_sender, handle)
}

struct NewBlockQueue {
    pub(crate) queue_rear: UnboundedSender<BlockId>,
    pub(crate) queue_front: UnboundedReceiver<BlockId>,
}

impl NewBlockQueue {
    async fn recv(&mut self) -> Option<BlockId> {
        let block = self.queue_front.recv().await;
        #[cfg(with_metrics)]
        crate::metrics::EXPORTER_NOTIFICATION_QUEUE_LENGTH.dec();
        block
    }

    fn push_back(&self, block_id: BlockId) {
        self.queue_rear
            .send(block_id)
            .expect("sender should never fail");
        #[cfg(with_metrics)]
        crate::metrics::EXPORTER_NOTIFICATION_QUEUE_LENGTH.inc();
    }
}

#[tokio::main(flavor = "current_thread")]
async fn start_block_processor<S, F>(
    storage: S,
    shutdown_signal: F,
    limits: LimitsConfig,
    options: NodeOptions,
    block_exporter_id: u32,
    new_block_queue: NewBlockQueue,
    destination_config: DestinationConfig,
) -> Result<(), ExporterError>
where
    S: Storage + Clone + Send + Sync + 'static,
    F: IntoFuture<Output = ()> + Clone + Send + Sync + 'static,
    <F as IntoFuture>::IntoFuture: Future<Output = ()> + Send + Sync + 'static,
{
    let startup_destinations = destination_config
        .destinations
        .iter()
        .map(|destination| destination.id())
        .collect::<Vec<_>>();
    let (mut block_processor_storage, mut exporter_storage) = BlockProcessorStorage::load(
        storage.clone(),
        block_exporter_id,
        startup_destinations.clone(),
        limits,
    )
    .await?;

    let startup_committee_destinations = if destination_config.committee_destination {
        // Load persisted committee destinations from storage if available
        // This may perform a fallback scan if no persisted blob ID exists
        let (persisted_committee_destinations, blob_id_to_persist) =
            match load_persisted_committee_destinations(
                &storage,
                &block_processor_storage,
                &mut exporter_storage,
            )
            .await
            {
                Some((destinations, blob_id)) => (Some(destinations), blob_id),
                None => (None, None),
            };

        // If we found a committee via fallback scan, persist it for future startups
        if let Some(blob_id) = blob_id_to_persist {
            tracing::info!(
                ?blob_id,
                "Persisting committee blob ID found via fallback scan"
            );
            block_processor_storage.set_latest_committee_blob(blob_id);
        }
        let latest_committee_destinations = persisted_committee_destinations.unwrap_or_default();
        tracing::info!(
            ?latest_committee_destinations,
            "Init committee exporters from persisted state"
        );
        latest_committee_destinations
            .into_iter()
            .collect::<HashSet<_>>()
    } else {
        HashSet::new()
    };

    let tracker = ExportersTracker::new(
        options,
        limits.work_queue_size.into(),
        shutdown_signal.clone(),
        exporter_storage.clone()?,
        destination_config.destinations.clone(),
        startup_committee_destinations,
    );

    let mut block_processor = BlockProcessor::new(
        tracker,
        block_processor_storage,
        new_block_queue,
        destination_config.committee_destination,
    );

    block_processor
        .run_with_shutdown(shutdown_signal, limits.persistence_period_ms)
        .await?;

    block_processor.pool_state().join_all().await;

    Ok(())
}

/// Loads the persisted committee destinations from storage.
/// If no committee blob ID is persisted, falls back to scanning the canonical blocks.
///
/// Returns:
/// - `None` if no committee is found
/// - `Some((destinations, None))` if loaded from persisted blob ID (no need to persist)
/// - `Some((destinations, Some(blob_id)))` if found via scan (caller should persist blob_id)
async fn load_persisted_committee_destinations<S>(
    storage: &S,
    block_processor_storage: &BlockProcessorStorage<S>,
    exporter_storage: &mut crate::storage::ExporterStorage<S>,
) -> Option<(Vec<DestinationId>, Option<BlobId>)>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // First, try to load from persisted committee blob ID
    if let Some(committee_blob_id) = block_processor_storage.get_latest_committee_blob() {
        tracing::info!(?committee_blob_id, "Found persisted committee blob ID");

        if let Some(destinations) = load_committee_from_blob(storage, committee_blob_id).await {
            return Some((destinations, None));
        }
    }

    // Fallback: scan backwards through canonical blocks to find the latest committee
    tracing::info!("No persisted committee blob ID, scanning canonical blocks...");
    scan_canonical_blocks_for_committee(storage, exporter_storage).await
}

/// Loads the committee destinations from a specific blob ID.
async fn load_committee_from_blob<S>(
    storage: &S,
    committee_blob_id: BlobId,
) -> Option<Vec<DestinationId>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let blob = match storage.read_blob(committee_blob_id).await {
        Ok(Some(blob)) => blob,
        Ok(None) => {
            tracing::error!(?committee_blob_id, "Committee blob ID not found in storage");
            return None;
        }
        Err(e) => {
            tracing::error!(
                ?committee_blob_id,
                error = ?e,
                "Failed to read committee blob"
            );
            return None;
        }
    };

    let committee: Committee = match bcs::from_bytes(blob.bytes()) {
        Ok(committee) => committee,
        Err(e) => {
            tracing::error!(
                ?committee_blob_id,
                error = ?e,
                "Failed to deserialize committee blob"
            );
            return None;
        }
    };

    let destinations: Vec<DestinationId> = committee
        .validator_addresses()
        .map(|(_, address)| DestinationId::validator(address.to_owned()))
        .collect();

    Some(destinations)
}

/// Scans backwards through canonical blocks to find the latest committee.
/// Returns the destinations and the blob ID that should be persisted for future startups.
async fn scan_canonical_blocks_for_committee<S>(
    storage: &S,
    exporter_storage: &mut crate::storage::ExporterStorage<S>,
) -> Option<(Vec<DestinationId>, Option<BlobId>)>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    use linera_base::identifiers::BlobType;
    use linera_execution::{system::AdminOperation, Operation, SystemOperation};

    let latest_index = exporter_storage.get_latest_index().await;
    if latest_index == 0 {
        tracing::info!("No blocks in canonical state to scan");
        return None;
    }

    tracing::info!(
        latest_index,
        "Scanning canonical blocks backwards for committee"
    );

    // Scan backwards from latest_index - 1 to 0
    for index in (0..latest_index).rev() {
        let (block_cert, _blob_ids) = match exporter_storage.get_block_with_blob_ids(index).await {
            Ok(result) => result,
            Err(e) => {
                tracing::warn!(
                    index,
                    error = ?e,
                    "Failed to read block at index during committee scan"
                );
                continue;
            }
        };

        // Check if this block has a CreateCommittee operation
        let committee_blob_id = block_cert.value().block().body.operations().find_map(|op| {
            if let Operation::System(boxed) = op {
                if let SystemOperation::Admin(AdminOperation::CreateCommittee {
                    blob_hash, ..
                }) = boxed.as_ref()
                {
                    return Some(BlobId::new(*blob_hash, BlobType::Committee));
                }
            }
            None
        });

        if let Some(blob_id) = committee_blob_id {
            tracing::info!(index, ?blob_id, "Found committee blob via backward scan");

            if let Some(destinations) = load_committee_from_blob(storage, blob_id).await {
                // Return the blob ID so caller can persist it
                return Some((destinations, Some(blob_id)));
            }
        }
    }

    tracing::info!("No committee found in canonical blocks");
    None
}

#[cfg(test)]
mod test {
    use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

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
        test::{make_child_block, make_first_block, BlockTestExt},
        types::{CertificateValue, ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_execution::{
        committee::{Committee, ValidatorState},
        system::AdminOperation,
        Operation, ResourceControlPolicy, SystemOperation,
    };
    use linera_rpc::{config::TlsConfig, NodeOptions};
    use linera_service::{
        cli_wrappers::local_net::LocalNet,
        config::{Destination, DestinationConfig, DestinationKind, LimitsConfig},
    };
    use linera_storage::{DbStorage, Storage};
    use linera_views::{memory::MemoryDatabase, ViewError};
    use test_case::test_case;
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    use super::start_block_processor_task;
    use crate::{
        common::{get_address, BlockId, CanonicalBlock},
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
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let destination_address = match destination.kind() {
            DestinationKind::Indexer => Destination::Indexer {
                port,
                tls: TlsConfig::ClearText,
                endpoint: "127.0.0.1".to_owned(),
            },
            DestinationKind::Validator => Destination::Validator {
                port,
                endpoint: "127.0.0.1".to_owned(),
            },
            DestinationKind::Logging => {
                unreachable!("Logging destination is not supported in tests")
            }
        };

        // make some blocks
        let (notification, state) = make_simple_state_with_blobs(&storage).await;

        let (notifier, handle) = start_block_processor_task(
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
            DestinationConfig {
                committee_destination: false,
                destinations: vec![destination_address],
            },
        );

        assert!(
            notifier.send(notification).is_ok(),
            "notifier should work as long as there exists a receiver to receive notifications"
        );

        sleep(Duration::from_secs(4)).await;

        for CanonicalBlock { blobs, block_hash } in state {
            assert!(destination.state().pin().contains(&block_hash));
            for blob in blobs {
                assert!(destination.blobs().pin().contains(&blob));
            }
        }

        cancellation_token.cancel();
        handle.join().unwrap()?;
        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_restart_persistence_and_faulty_destination_restart() -> Result<(), anyhow::Error>
    {
        let mut destinations = Vec::new();
        let cancellation_token = CancellationToken::new();
        let _indexer = spawn_dummy_indexer(&mut destinations, &cancellation_token).await?;
        let faulty_indexer = spawn_faulty_indexer(&mut destinations, &cancellation_token).await?;
        let validator = spawn_dummy_validator(&mut destinations, &cancellation_token).await?;
        let faulty_validator =
            spawn_faulty_validator(&mut destinations, &cancellation_token).await?;

        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;

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
            DestinationConfig {
                committee_destination: false,
                destinations: destinations.clone(),
            },
        );

        assert!(
            notifier.send(notification).is_ok(),
            "notifier should work as long as there exists a receiver to receive notifications"
        );

        sleep(Duration::from_secs(4)).await;

        child.cancel();
        // handle.join().unwrap()?;

        let context = storage.block_exporter_context(0).await?;
        let destination_ids = destinations.iter().map(|d| d.id()).collect::<Vec<_>>();
        let (_, _, destination_states) =
            BlockExporterStateView::initiate(context.clone(), destination_ids.clone()).await?;
        for (i, destination) in destination_ids.iter().enumerate() {
            let state = destination_states.load_state(destination);
            // We created destinations such that odd ones were faulty.
            if i % 2 == 0 {
                assert_eq!(state.load(Ordering::Acquire), 2);
            } else {
                assert_eq!(state.load(Ordering::Acquire), 0);
            }
        }

        assert!(validator.duplicate_blocks.is_empty());

        tracing::info!("restarting block processor task with faulty destinations fixed");

        faulty_indexer.unset_faulty();
        faulty_validator.unset_faulty();

        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());

        // restart
        let (_notifier, handle) = start_block_processor_task(
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
            DestinationConfig {
                destinations: destinations.clone(),
                committee_destination: false,
            },
        );

        sleep(Duration::from_secs(4)).await;

        child.cancel();
        handle.join().unwrap()?;

        let (_, _, destination_states) =
            BlockExporterStateView::initiate(context.clone(), destination_ids).await?;
        for destination in destinations {
            assert_eq!(
                destination_states
                    .load_state(&destination.id())
                    .load(Ordering::Acquire),
                2
            );
        }

        assert!(validator.duplicate_blocks.is_empty());
        assert!(faulty_validator.duplicate_blocks.is_empty());

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_committee_destination() -> anyhow::Result<()> {
        tracing::info!("Starting test_committee_destination test");

        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let test_chain = TestChain::new(storage.clone());
        let cancellation_token = CancellationToken::new();
        let child = cancellation_token.child_token();
        let signal = ExporterCancellationSignal::new(child.clone());

        let mut destinations = vec![];
        let dummy_validator = spawn_dummy_validator(&mut destinations, &cancellation_token).await?;
        let destination = destinations[0].clone();
        let validator_state = ValidatorState {
            network_address: destination.address(),
            votes: 0,
            account_public_key: AccountPublicKey::test_key(0),
        };
        let (notifier, block_processor_handle) = start_block_processor_task(
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
            DestinationConfig {
                committee_destination: true,
                destinations: vec![],
            },
        );

        let mut single_validator = BTreeMap::new();
        single_validator.insert(Secp256k1PublicKey::test_key(0), validator_state);

        let confirmed_certificate = test_chain
            .publish_committee(&single_validator, None)
            .await
            .expect("Failed to publish committee");

        let first_notification = BlockId::from_confirmed_block(confirmed_certificate.value());
        notifier.send(first_notification)?;
        sleep(Duration::from_secs(4)).await;

        {
            let pinned = dummy_validator.state.pin();
            assert!(pinned.contains(&first_notification.hash));
        }
        // We expect the validator to receive the confired certificate only once.
        {
            let pinned = dummy_validator.duplicate_blocks.pin();
            assert!(pinned.get(&first_notification.hash).is_none());
        }

        ///////////
        // Add new validator to the committee.
        ///////////
        let second_dummy = spawn_dummy_validator(&mut destinations, &cancellation_token).await?;
        let destination = destinations[1].clone();
        let validator_state = ValidatorState {
            network_address: destination.address(),
            votes: 0,
            account_public_key: AccountPublicKey::test_key(1),
        };
        let mut two_validators = single_validator.clone();
        two_validators.insert(Secp256k1PublicKey::test_key(1), validator_state);
        let add_validator_certificate = test_chain
            .publish_committee(&two_validators, Some(confirmed_certificate.value().clone()))
            .await
            .expect("Failed to publish new committee");
        let second_notification = BlockId::from_confirmed_block(add_validator_certificate.value());
        notifier.send(second_notification)?;
        sleep(Duration::from_secs(4)).await;

        {
            let pinned = second_dummy.state.pin();
            assert!(pinned.contains(&second_notification.hash));
        }
        // We expect the new validator to receive the new confirmed certificate only once.
        {
            let pinned = second_dummy.duplicate_blocks.pin();
            assert!(pinned.get(&second_notification.hash).is_none());
        }
        // The first certificate should not be duplicated.
        {
            let pinned = second_dummy.duplicate_blocks.pin();
            assert!(pinned.get(&first_notification.hash).is_none());
        }

        // The first validator should receive the new committee as well.
        {
            let pinned = dummy_validator.state.pin();
            assert!(pinned.contains(&second_notification.hash));
        }
        // We expect the first validator to receive the new confirmed certificate only once.
        {
            let pinned = dummy_validator.duplicate_blocks.pin();
            assert!(pinned.get(&second_notification.hash).is_none());
        }

        ///////////
        // Remove the validator from the committee.
        ///////////

        let mut new_validators = two_validators.clone();
        new_validators.remove(&Secp256k1PublicKey::test_key(0));

        let remove_validator_certificate = test_chain
            .publish_committee(
                &new_validators,
                Some(add_validator_certificate.value().clone()),
            )
            .await
            .expect("Failed to publish new committee");

        let third_notification =
            BlockId::from_confirmed_block(remove_validator_certificate.value());
        notifier.send(third_notification)?;
        sleep(Duration::from_secs(4)).await;
        // The first validator should not receive the new confirmed certificate.
        {
            let pinned = dummy_validator.state.pin();
            assert!(!pinned.contains(&third_notification.hash));
        }
        // We expect the first validator to receive the new confirmed certificate only once.
        {
            let pinned = dummy_validator.duplicate_blocks.pin();
            assert!(pinned.get(&third_notification.hash).is_none());
        }
        // The second validator should receive the new confirmed certificate.
        {
            let pinned = second_dummy.state.pin();
            assert!(pinned.contains(&third_notification.hash));
        }
        // We expect the second validator to receive the new confirmed certificate only once.
        {
            let pinned = second_dummy.duplicate_blocks.pin();
            assert!(pinned.get(&third_notification.hash).is_none());
        }

        cancellation_token.cancel();
        block_processor_handle.join().unwrap()?;
        Ok(())
    }

    struct TestChain<S> {
        chain_description: ChainDescription,
        storage: S,
    }

    impl<S> TestChain<S> {
        fn new(storage: S) -> Self {
            let chain_description = ChainDescription::new(
                ChainOrigin::Root(0),
                InitialChainConfig {
                    ownership: Default::default(),
                    epoch: Default::default(),
                    balance: Default::default(),
                    application_permissions: Default::default(),
                    min_active_epoch: Epoch::ZERO,
                    max_active_epoch: Epoch::ZERO,
                },
                Timestamp::now(),
            );
            Self {
                chain_description,
                storage,
            }
        }

        // Constructs a new block, with the blob containing the committee.
        async fn publish_committee(
            &self,
            validators: &BTreeMap<Secp256k1PublicKey, ValidatorState>,
            prev_block: Option<ConfirmedBlock>,
        ) -> Result<ConfirmedBlockCertificate, ViewError>
        where
            S: Storage + Clone + Send + Sync + 'static,
        {
            let committee = Committee::new(validators.clone(), ResourceControlPolicy::testnet());
            let chain_id = self.chain_description.id();
            let chain_blob = Blob::new_chain_description(&self.chain_description);

            let committee_blob = Blob::new(BlobContent::new_committee(bcs::to_bytes(&committee)?));
            let proposed_block = if let Some(parent_block) = prev_block {
                make_child_block(&parent_block).with_operation(Operation::System(Box::new(
                    SystemOperation::Admin(AdminOperation::CreateCommittee {
                        epoch: parent_block.epoch().try_add_one().unwrap(),
                        blob_hash: committee_blob.id().hash,
                    }),
                )))
            } else {
                make_first_block(chain_id).with_operation(Operation::System(Box::new(
                    SystemOperation::Admin(AdminOperation::CreateCommittee {
                        epoch: Epoch::ZERO,
                        blob_hash: committee_blob.id().hash,
                    }),
                )))
            };
            let blobs = vec![chain_blob, committee_blob];
            let block = BlockExecutionOutcome {
                blobs: vec![blobs.clone()],
                ..Default::default()
            }
            .with(proposed_block);

            let confirmed_block = ConfirmedBlock::new(block);
            let certificate = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);
            self.storage
                .write_blobs_and_certificate(blobs.as_ref(), &certificate)
                .await?;

            Ok(certificate)
        }
    }

    async fn spawn_dummy_indexer(
        destinations: &mut Vec<Destination>,
        token: &CancellationToken,
    ) -> anyhow::Result<DummyIndexer> {
        let port = get_free_port().await?;
        let destination = DummyIndexer::default();
        tokio::spawn(destination.clone().start(port, token.clone()));
        LocalNet::ensure_grpc_server_has_started("dummy indexer", port as usize, "http").await?;
        let destination_address = Destination::Indexer {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
        };

        destinations.push(destination_address);
        Ok(destination)
    }

    async fn spawn_dummy_validator(
        destinations: &mut Vec<Destination>,
        token: &CancellationToken,
    ) -> anyhow::Result<DummyValidator> {
        let port = get_free_port().await?;
        let destination = DummyValidator::new(port);
        tokio::spawn(destination.clone().start(port, token.clone()));
        LocalNet::ensure_grpc_server_has_started("dummy validator", port as usize, "http").await?;
        let destination_address = Destination::Validator {
            port,
            endpoint: get_address(port as u16).ip().to_string(),
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
        let destination_address = Destination::Indexer {
            port,
            tls: TlsConfig::ClearText,
            endpoint: "127.0.0.1".to_owned(),
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
        LocalNet::ensure_grpc_server_has_started("faulty validator", port as usize, "http").await?;
        let destination_address = Destination::Validator {
            port,
            endpoint: "127.0.0.1".to_owned(),
        };

        destinations.push(destination_address);
        Ok(destination)
    }

    /// Tests that when no committee blob ID is persisted, the fallback scan
    /// through canonical blocks finds the latest committee.
    #[tokio::test]
    async fn test_fallback_committee_scan_finds_committee() -> anyhow::Result<()> {
        use std::collections::BTreeMap;

        use linera_base::{
            crypto::{AccountPublicKey, CryptoHash},
            data_types::{Blob, BlobContent, Epoch, Round},
            identifiers::{BlobId, BlobType, ChainId},
        };
        use linera_chain::{
            data_types::BlockExecutionOutcome,
            test::{make_child_block, make_first_block, BlockTestExt},
            types::{ConfirmedBlock, ConfirmedBlockCertificate},
        };
        use linera_execution::{
            committee::{Committee, ValidatorState},
            system::AdminOperation,
            Operation, ResourceControlPolicy, SystemOperation,
        };

        use super::load_persisted_committee_destinations;
        use crate::storage::BlockProcessorStorage;

        // Create a test committee with a specific network address
        let validator_key = linera_base::crypto::ValidatorPublicKey::test_key(1);
        let account_key = AccountPublicKey::test_key(1);
        let expected_address = "Tcp:validator1:9000";
        let mut validators = BTreeMap::new();
        validators.insert(
            validator_key,
            ValidatorState {
                network_address: expected_address.to_string(),
                votes: 100,
                account_public_key: account_key,
            },
        );
        let committee = Committee::new(validators, ResourceControlPolicy::default());
        let committee_bytes = bcs::to_bytes(&committee)?;
        let committee_blob = Blob::new(BlobContent::new_committee(committee_bytes));
        let committee_blob_hash = CryptoHash::new(committee_blob.content());
        let _committee_blob_id = BlobId::new(committee_blob_hash, BlobType::Committee);

        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;

        // Store the committee blob
        storage.write_blobs(&[committee_blob]).await?;

        // Create blocks including one with CreateCommittee operation
        let chain_id = ChainId(CryptoHash::test_hash("admin_chain"));
        let create_committee_op = Operation::System(Box::new(SystemOperation::Admin(
            AdminOperation::CreateCommittee {
                epoch: Epoch::ZERO,
                blob_hash: committee_blob_hash,
            },
        )));

        // First block (no committee)
        let block1 =
            ConfirmedBlock::new(BlockExecutionOutcome::default().with(make_first_block(chain_id)));
        let cert1 = ConfirmedBlockCertificate::new(block1.clone(), Round::Fast, vec![]);
        storage.write_blobs_and_certificate(&[], &cert1).await?;

        // Second block with CreateCommittee
        let block2 = ConfirmedBlock::new(
            BlockExecutionOutcome::default()
                .with(make_child_block(&block1).with_operation(create_committee_op)),
        );
        let cert2 = ConfirmedBlockCertificate::new(block2.clone(), Round::Fast, vec![]);
        storage.write_blobs_and_certificate(&[], &cert2).await?;

        // === FIRST SESSION: Process blocks and save ===
        // This simulates an existing database from before the persistence feature was added
        {
            let (mut block_processor_storage, _) =
                BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                    .await?;

            let block1_id = crate::common::BlockId::from_confirmed_block(&block1);
            let block2_id = crate::common::BlockId::from_confirmed_block(&block2);

            block_processor_storage.index_block(&block1_id).await?;
            block_processor_storage
                .push_block(crate::common::CanonicalBlock::new(block1_id.hash, &[]))
                .await;

            block_processor_storage.index_block(&block2_id).await?;
            block_processor_storage
                .push_block(crate::common::CanonicalBlock::new(block2_id.hash, &[]))
                .await;

            // Save but don't persist the committee blob ID
            block_processor_storage.save().await?;

            // Verify no persisted committee blob ID
            assert!(
                block_processor_storage
                    .get_latest_committee_blob()
                    .is_none(),
                "Expected no persisted committee blob ID"
            );
        }

        // === SECOND SESSION: Reload and test fallback scan ===
        // This simulates a restart with the new code
        let (mut block_processor_storage, mut exporter_storage) =
            BlockProcessorStorage::load(storage.clone(), 0, vec![], LimitsConfig::default())
                .await?;

        // Verify still no persisted committee blob ID after reload
        assert!(
            block_processor_storage
                .get_latest_committee_blob()
                .is_none(),
            "Expected no persisted committee blob ID after reload"
        );

        // Now call load_persisted_committee_destinations
        // This should trigger the fallback scan
        let result = load_persisted_committee_destinations(
            &storage,
            &block_processor_storage,
            &mut exporter_storage,
        )
        .await;

        // Should find the committee via fallback scan
        assert!(
            result.is_some(),
            "Expected fallback scan to find the committee"
        );

        let (destinations, found_blob_id) = result.unwrap();
        assert!(
            found_blob_id.is_some(),
            "Expected blob ID to be returned for persistence"
        );
        assert_eq!(destinations.len(), 1);
        assert_eq!(destinations[0].address(), expected_address);
        {
            block_processor_storage.set_latest_committee_blob(found_blob_id.unwrap());

            let latest_committee_blob = block_processor_storage
                .get_latest_committee_blob()
                .expect("Expected persisted committee blob ID after scan");
            assert_eq!(
                found_blob_id.unwrap(),
                latest_committee_blob,
                "Returned blob ID should match persisted blob ID"
            );
        }
        Ok(())
    }

    /// Regression test: after the block processor flushes (saves) blocks,
    /// the exporter storage must see the updated state (latest_index and
    /// block contents). Under the old `clone_unchecked()` sharing model,
    /// the exporter held a diverged copy whose `stored_count` would not
    /// reflect the flush, causing stale reads.
    #[test_log::test(tokio::test)]
    async fn test_shared_state_consistent_after_flush() -> anyhow::Result<()> {
        let storage = DbStorage::<MemoryDatabase, _>::make_test_storage(None).await;
        let chain_id = linera_base::identifiers::ChainId(
            linera_base::crypto::CryptoHash::test_hash("shared_state_test"),
        );

        // Create two blocks and persist them to storage.
        let block1 =
            ConfirmedBlock::new(BlockExecutionOutcome::default().with(make_first_block(chain_id)));
        let cert1 = ConfirmedBlockCertificate::new(block1.clone(), Round::Fast, vec![]);
        storage.write_blobs_and_certificate(&[], &cert1).await?;

        let block2 =
            ConfirmedBlock::new(BlockExecutionOutcome::default().with(make_child_block(&block1)));
        let cert2 = ConfirmedBlockCertificate::new(block2.clone(), Round::Fast, vec![]);
        storage.write_blobs_and_certificate(&[], &cert2).await?;

        let (mut block_processor_storage, exporter_storage) =
            crate::storage::BlockProcessorStorage::load(
                storage.clone(),
                0,
                vec![],
                LimitsConfig::default(),
            )
            .await?;

        // Initially, no blocks in canonical state.
        assert_eq!(exporter_storage.get_latest_index().await, 0);

        // Index and push two blocks via the block processor.
        let block1_id = crate::common::BlockId::from_confirmed_block(&block1);
        let block2_id = crate::common::BlockId::from_confirmed_block(&block2);

        block_processor_storage.index_block(&block1_id).await?;
        block_processor_storage
            .push_block(CanonicalBlock::new(block1_id.hash, &[]))
            .await;

        block_processor_storage.index_block(&block2_id).await?;
        block_processor_storage
            .push_block(CanonicalBlock::new(block2_id.hash, &[]))
            .await;

        // Under the old `clone_unchecked()` model, `count` was a plain usize
        // copied at clone time. `push()` only incremented the block processor's
        // copy, so the exporter's `get_latest_index()` would still return 0 here.
        assert_eq!(exporter_storage.get_latest_index().await, 2);

        // Flush (save) persists the blocks.
        block_processor_storage.save().await?;

        // After flush, the exporter must still see the correct count
        // and be able to retrieve the blocks by index.
        assert_eq!(exporter_storage.get_latest_index().await, 2);

        let (retrieved_cert, _blob_ids) = exporter_storage.get_block_with_blob_ids(0).await?;
        assert_eq!(retrieved_cert.hash(), cert1.hash());

        let (retrieved_cert, _blob_ids) = exporter_storage.get_block_with_blob_ids(1).await?;
        assert_eq!(retrieved_cert.hash(), cert2.hash());

        Ok(())
    }
}
