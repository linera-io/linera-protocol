// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(with_testing)]
use std::collections::BTreeSet;
use std::net::SocketAddr;

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId, listen_for_shutdown_signals,
};
#[cfg(with_testing)]
use linera_base::{data_types::Blob, identifiers::BlobId};
#[cfg(with_testing)]
use linera_chain::types::{CertificateValue, ConfirmedBlock};
use linera_client::config::DestinationConfig;
use linera_core::worker::Reason;
use linera_rpc::{
    config::ExporterServiceConfig,
    grpc::api::{
        notifier_service_server::{NotifierService, NotifierServiceServer},
        Notification,
    },
};
use linera_sdk::views::{RootView, View};
use linera_service::storage::Runnable;
use linera_storage::Storage;
#[cfg(test)]
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

#[cfg(with_testing)]
use crate::state::StorageExt;
use crate::{block_processor::BlockProcessor, state::BlockExporterStateView, ExporterError};

#[derive(Debug)]
pub(super) struct ExporterContext {
    id: u32,
    service_config: ExporterServiceConfig,
    destination_config: DestinationConfig,
}

pub(crate) struct ExporterService<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    state: Mutex<BlockExporterStateView<S::BlockExporterContext>>,
    destination_config: DestinationConfig,
    storage: S,
    #[cfg(test)]
    debug_destination: Option<UnboundedSender<Summary>>,
}

#[async_trait]
impl<S> NotifierService for ExporterService<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn notify(&self, request: Request<Notification>) -> Result<Response<()>, Status> {
        let notification = request.into_inner();
        let (chain_id, block_height, block_hash) =
            parse_notification(notification).map_err(|e| Status::from_error(e.into()))?;

        #[allow(unused)]
        let batch = {
            let mut guard = self.state.lock().await;
            guard
                .initialize_chain(&chain_id, (block_height, block_hash))
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            let mut block_processor = BlockProcessor::new(0, &self.storage, guard);
            let batch = block_processor
                .process_block(block_hash)
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            let mut guard = block_processor.destructor();
            guard
                .save()
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            batch
        };

        // after implementation of future destinations
        // this will be offloaded to a seperate thread.
        #[cfg(with_testing)]
        {
            let batch = self
                .storage
                .read_batch(batch)
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            let summary = batch.into_summary();

            if cfg!(feature = "test") {
                tracing::debug!(
                    "Dispatching batch from block exporter batch summary: {:?}",
                    summary
                );
            }

            #[cfg(test)]
            if let Some(tx) = &self.debug_destination {
                let _ = tx.send(summary);
            }
        }

        Ok(Response::new(()))
    }
}

impl<S> ExporterService<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    async fn from_context(
        exporter_context: &ExporterContext,
        storage: S,
    ) -> Result<Self, ExporterError> {
        let storage_context = storage
            .block_exporter_context(exporter_context.id)
            .await
            .map_err(ExporterError::StateError)?;
        let state = BlockExporterStateView::load(storage_context).await?;
        let destination_config = exporter_context.destination_config.clone();
        Ok(Self {
            state: Mutex::new(state),
            destination_config,
            storage,
            #[cfg(test)]
            debug_destination: None,
        })
    }

    pub async fn run(
        self,
        cancellation_token: CancellationToken,
        port: u16,
    ) -> core::result::Result<(), ExporterError> {
        info!("Linera exporter is running.");
        self.start_notification_server(port, cancellation_token)
            .await
    }

    #[cfg(test)]
    fn with_redirection_buffer(mut self, buffer: UnboundedSender<Summary>) -> Self {
        self.debug_destination = Some(buffer);
        self
    }
}

#[async_trait]
impl Runnable for ExporterContext {
    type Output = Result<(), ExporterError>;

    async fn run<S>(self, storage: S) -> Self::Output
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let shutdown_notifier = CancellationToken::new();
        tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));
        let port = self.service_config.port;
        let service = ExporterService::from_context(&self, storage).await?;
        service.run(shutdown_notifier, port).await
    }
}

impl ExporterContext {
    pub(super) fn new(
        id: u32,
        service_config: ExporterServiceConfig,
        destination_config: DestinationConfig,
    ) -> ExporterContext {
        Self {
            id,
            service_config,
            destination_config,
        }
    }
}

fn parse_notification(
    notification: Notification,
) -> core::result::Result<(ChainId, BlockHeight, CryptoHash), ExporterError> {
    let chain_id = notification
        .chain_id
        .ok_or(ExporterError::BadNotification)?;
    let reason = bincode::deserialize::<Reason>(&notification.reason)
        .map_err(|_| ExporterError::BadNotification)?;
    if let Reason::NewBlock { height, hash } = reason {
        return Ok((
            chain_id
                .try_into()
                .map_err(|_| ExporterError::BadNotification)?,
            height,
            hash,
        ));
    }

    Err(ExporterError::BadNotification)
}

fn get_address(port: u16) -> SocketAddr {
    SocketAddr::from(([0, 0, 0, 0], port))
}

impl<S> ExporterService<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    pub async fn start_notification_server(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> core::result::Result<(), ExporterError> {
        let endpoint = get_address(port);
        info!(
            "Starting linera_exporter_service on endpoint = {}",
            endpoint
        );

        let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NotifierServiceServer<Self>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(NotifierServiceServer::new(self))
            .serve_with_shutdown(endpoint, cancellation_token.cancelled_owned())
            .await
            .expect("a running notification server");

        if cfg!(with_testing) {
            Ok(())
        } else {
            Err(ExporterError::GenericError(
                "Service should run forever.".to_string().into(),
            ))
        }
    }
}

#[cfg(with_testing)]
#[derive(Debug, PartialEq)]
pub struct Summary {
    chain_id: ChainId,
    block_height: BlockHeight,
    block_hash: CryptoHash,
    block_dependencies: BTreeSet<CryptoHash>,
    blob_dependencies: BTreeSet<BlobId>,
    block_byte_size: usize,
    block_dependencies_byte_size: usize,
    blob_dependencies_byte_size: usize,
}

#[cfg(with_testing)]
impl Summary {
    pub fn new(block: &ConfirmedBlock) -> Summary {
        let chain_id = block.chain_id();
        let block_height = block.height();
        let block_hash = block.hash();
        let block_byte_size = bcs::serialized_size(block).unwrap();

        Summary {
            chain_id,
            block_height,
            block_hash,
            block_dependencies: BTreeSet::new(),
            blob_dependencies: BTreeSet::new(),
            block_byte_size,
            block_dependencies_byte_size: 0,
            blob_dependencies_byte_size: 0,
        }
    }

    pub fn add_blob(&mut self, blob: &Blob) {
        let byte_size = bcs::serialized_size(blob).unwrap();
        if self.blob_dependencies.insert(blob.id()) {
            self.blob_dependencies_byte_size += byte_size;
        }
    }

    pub fn add_block(&mut self, block: &ConfirmedBlock) {
        let byte_size = bcs::serialized_size(block).unwrap();
        if self.block_dependencies.insert(block.hash()) {
            self.block_dependencies_byte_size += byte_size;
        }
    }
}

#[cfg(test)]
mod test {
    use core::iter;
    use std::collections::{BTreeMap, HashSet};

    use linera_base::{
        crypto::CryptoHash,
        data_types::{Amount, Round, Timestamp},
        port::get_free_port,
    };
    use linera_chain::{
        data_types::{
            BlockExecutionOutcome, IncomingBundle, MessageAction, MessageBundle, OperationResult,
            Origin,
        },
        test::{make_first_block, BlockTestExt},
        types::{ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_rpc::grpc::api::notifier_service_client::NotifierServiceClient;
    use linera_service::cli_wrappers::local_net::LocalNet;
    use linera_storage::DbStorage;
    use linera_views::memory::MemoryStore;
    use proptest::bits::u32;
    use tonic::transport::Channel;

    use super::*;
    use crate::state::Batch;

    struct ProtocolState {
        chains: BTreeMap<ChainId, Vec<ConfirmedBlock>>,
        blobs: BTreeMap<BlobId, Blob>,
        synchronized_blocks: HashSet<CryptoHash>,
        synchronized_blobs: HashSet<BlobId>,
    }

    impl ProtocolState {
        fn new() -> Self {
            Self {
                chains: BTreeMap::new(),
                blobs: BTreeMap::new(),
                synchronized_blocks: HashSet::new(),
                synchronized_blobs: HashSet::new(),
            }
        }

        // this function populates the state based on a repeatable pattern                                   A - 1   ->  2   ->  3   ->  4
        // the idea is to produce a densely connected directed acyclic graph                                     ↓↙↙↙  ↘↓↙↙ ↘↘↓↙  ↘↘↘↓
        // blocks in chains with even index are directed linearly                                            B   1       2       3       4
        // same with even index also have parallel ancestors at same height                                      ↓       ↓       ↓       ↓
        // in the previous chain with odd index.                                                             C - 1   ->  2   ->  3   ->  4
        // blocks in chains with odd index are densely connected
        // with every block in the last even chain
        fn populate_protocol_state(self, num_of_chains: u32, blocks_per_chain: usize) -> Self {
            let mut state = ProtocolState::new();

            // first chain
            let chain_id = ChainId::root(0);
            state.chains.insert(chain_id, Vec::new());
            state.add_block(0, chain_id, false, &[]);

            for height in 1..blocks_per_chain {
                state.add_block(height, chain_id, true, &[]);
            }

            // secondary chains
            for chain in 1..num_of_chains {
                let chain_id = ChainId::root(chain);
                let last_chain_id = ChainId::root(chain - 1);
                let (edge_last_block, mut edges) = if chain % 2 == 1 {
                    let mut i = 0;
                    let edges = iter::repeat_with(|| {
                        let height = i;
                        i += 1;
                        (last_chain_id, height)
                    })
                    .take(blocks_per_chain);
                    (false, edges.collect::<Vec<_>>())
                } else {
                    (true, vec![])
                };

                state.chains.insert(chain_id, Vec::new());

                for height in 0..blocks_per_chain {
                    if chain % 2 == 0 {
                        edges = vec![(last_chain_id, height)]
                    }

                    state.add_block(height, chain_id, edge_last_block, edges.as_ref());
                }
            }

            state
        }

        fn add_block(
            &mut self,
            block_height: usize,
            chain_id: ChainId,
            edge_last_block: bool,
            edges: &[(ChainId, usize)],
        ) {
            let mut proposed_block = if edge_last_block && block_height != 0 {
                let incoming_bundle = self.incoming_bundle(chain_id, block_height - 1);
                make_first_block(chain_id).with_incoming_bundle(incoming_bundle)
            } else {
                make_first_block(chain_id)
            };

            for edge in edges {
                let incoming_bundle = self.incoming_bundle(edge.0, edge.1);
                proposed_block = proposed_block.with_incoming_bundle(incoming_bundle);
            }

            let blob = Blob::new_data(format!("{chain_id}:{block_height}").as_bytes());
            let block = BlockExecutionOutcome {
                messages: vec![Vec::new()],
                previous_message_blocks: BTreeMap::new(),
                state_hash: CryptoHash::test_hash("state"),
                oracle_responses: vec![Vec::new()],
                events: vec![Vec::new()],
                blobs: vec![vec![blob.clone()]],
                operation_results: vec![OperationResult::default()],
            }
            .with(proposed_block);
            let confirmed_block = ConfirmedBlock::new(block);
            self.chains
                .get_mut(&chain_id)
                .unwrap()
                .push(confirmed_block);
            self.blobs.insert(blob.id(), blob);
        }

        fn incoming_bundle(&self, chain_id: ChainId, block_height: usize) -> IncomingBundle {
            IncomingBundle {
                origin: Origin::chain(chain_id),
                bundle: MessageBundle {
                    height: (block_height as u64).into(),
                    timestamp: Timestamp::now(),
                    certificate_hash: self
                        .chains
                        .get(&chain_id)
                        .unwrap()
                        .get(block_height)
                        .unwrap()
                        .hash(),
                    transaction_index: 0,
                    messages: vec![],
                },
                action: MessageAction::Accept,
            }
        }

        async fn synchronize_storage_with_block<S>(
            &self,
            chain_id: ChainId,
            block_height: usize,
            storage: S,
        ) -> Result<(), ExporterError>
        where
            S: Storage + Clone + Send + Sync + 'static,
        {
            let block = self
                .chains
                .get(&chain_id)
                .ok_or(ExporterError::GenericError(
                    "chain not found in the protocol state".into(),
                ))?
                .get(block_height)
                .ok_or(ExporterError::GenericError(
                    "block not found in the protocol state".into(),
                ))?;
            let certificate = ConfirmedBlockCertificate::new(block.clone(), Round::Fast, vec![]);
            storage
                .write_blobs_and_certificate(&[], &certificate)
                .await?;

            Ok(())
        }

        async fn synchronize_storage_with_blob<S>(
            &self,
            blob_id: BlobId,
            storage: S,
        ) -> Result<(), ExporterError>
        where
            S: Storage + Clone + Send + Sync + 'static,
        {
            let blob = self.blobs.get(&blob_id).ok_or(ExporterError::GenericError(
                "blob not found in the protocol state".into(),
            ))?;
            storage.write_blob(blob).await?;
            Ok(())
        }

        async fn synchronize_storage_with_state<S>(&self, storage: S) -> Result<(), ExporterError>
        where
            S: Storage + Clone + Send + Sync + 'static,
        {
            for (chain_id, chain) in &self.chains {
                for block_height in 0..chain.len() {
                    self.synchronize_storage_with_block(*chain_id, block_height, storage.clone())
                        .await?;
                }
            }

            for blob_id in self.blobs.keys() {
                self.synchronize_storage_with_blob(*blob_id, storage.clone())
                    .await?;
            }

            Ok(())
        }

        async fn synchronize_block_with_exporter(
            &mut self,
            chain_id: u32,
            block_height: usize,
            mut client: NotifierServiceClient<Channel>,
        ) -> anyhow::Result<Batch> {
            let current_chain_id = ChainId::root(chain_id);
            let block = &self.chains.get(&current_chain_id).unwrap()[block_height];
            let hash = block.hash();
            let blob_id = block.required_blob_ids().pop_first().unwrap();
            self.synchronized_blocks.insert(hash);
            self.synchronized_blobs.insert(blob_id);

            let blob = self.blobs.get(&blob_id).unwrap().clone();
            let mut batch = Batch::new(block.clone());
            batch.add_blob(blob);

            let reason = Reason::NewBlock {
                height: block.height(),
                hash: block.hash(),
            };
            let request = tonic::Request::new(Notification {
                chain_id: Some(block.chain_id().into()),
                reason: bincode::serialize(&reason).unwrap(),
            });

            let _response = client.notify(request).await?;

            if chain_id == 0 {
                let chain = ChainId::root(chain_id);
                for i in 0..block_height {
                    let block = &self.chains.get(&chain).unwrap()[i];
                    let hash = block.hash();
                    let blob_id = block.required_blob_ids().pop_first().unwrap();
                    let blob = self.blobs.get(&blob_id).unwrap().clone();
                    if self.synchronized_blocks.insert(hash) {
                        batch.add_block(block.clone());
                    }

                    if self.synchronized_blobs.insert(blob_id) {
                        batch.add_blob(blob);
                    }
                }

                return Ok(batch);
            }

            let mut chains = (0..chain_id + 1).map(ChainId::root).collect::<Vec<_>>();

            if chain_id % 2 == 0 {
                let last_chain = chains.pop().unwrap();
                for i in 0..block_height {
                    let block = &self.chains.get(&last_chain).unwrap()[i];
                    let hash = block.hash();
                    let blob_id = block.required_blob_ids().pop_first().unwrap();
                    let blob = self.blobs.get(&blob_id).unwrap().clone();
                    if self.synchronized_blocks.insert(hash) {
                        batch.add_block(block.clone());
                    }

                    if self.synchronized_blobs.insert(blob_id) {
                        batch.add_blob(blob);
                    }
                }

                let second_last_chain = chains.pop().unwrap();
                for i in 0..block_height + 1 {
                    let block = &self.chains.get(&second_last_chain).unwrap()[i];
                    let hash = block.hash();
                    let blob_id = block.required_blob_ids().pop_first().unwrap();
                    let blob = self.blobs.get(&blob_id).unwrap().clone();
                    if self.synchronized_blocks.insert(hash) {
                        batch.add_block(block.clone());
                    }

                    if self.synchronized_blobs.insert(blob_id) {
                        batch.add_blob(blob);
                    }
                }

                for chain in chains {
                    let blocks = self.chains.get(&chain).unwrap();
                    for block in blocks {
                        let hash = block.hash();
                        let blob_id = block.required_blob_ids().pop_first().unwrap();
                        let blob = self.blobs.get(&blob_id).unwrap().clone();
                        if self.synchronized_blocks.insert(hash) {
                            batch.add_block(block.clone());
                        }

                        if self.synchronized_blobs.insert(blob_id) {
                            batch.add_blob(blob);
                        }
                    }
                }
            } else {
                let _ = chains.pop().unwrap();
                for chain in chains {
                    let blocks = self.chains.get(&chain).unwrap();
                    for block in blocks {
                        let hash = block.hash();
                        let blob_id = block.required_blob_ids().pop_first().unwrap();
                        let blob = self.blobs.get(&blob_id).unwrap().clone();
                        if self.synchronized_blocks.insert(hash) {
                            batch.add_block(block.clone());
                        }

                        if self.synchronized_blobs.insert(blob_id) {
                            batch.add_blob(blob);
                        }
                    }
                }
            }

            Ok(batch)
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_notification_service() -> anyhow::Result<()> {
        tracing::info!("Starting test: test_notification_service");
        let port = get_free_port().await?;
        let endpoint = format!("127.0.0.1:{port}");

        let cancellation_token = CancellationToken::new();

        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let service_config = ExporterServiceConfig {
            host: "127.0.0.1".to_string(),
            port,
        };
        let destination_config = DestinationConfig {
            destinations: vec![],
        };

        let block = BlockExecutionOutcome {
            messages: vec![Vec::new()],
            previous_message_blocks: BTreeMap::new(),
            state_hash: CryptoHash::test_hash("state"),
            oracle_responses: vec![Vec::new()],
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            operation_results: vec![OperationResult::default()],
        }
        .with(
            make_first_block(ChainId::root(1)).with_simple_transfer(ChainId::root(1), Amount::ONE),
        );
        let confirmed_block = ConfirmedBlock::new(block);
        let certificate = ConfirmedBlockCertificate::new(confirmed_block, Round::Fast, vec![]);
        storage
            .write_blobs_and_certificate(&[], &certificate)
            .await?;

        let context = ExporterContext::new(0, service_config, destination_config);
        let service = ExporterService::from_context(&context, storage).await?;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let service = service.with_redirection_buffer(tx);
        tokio::spawn(service.run(cancellation_token.clone(), port));

        LocalNet::ensure_grpc_server_has_started("test server", port as usize, "http").await?;

        let mut client = NotifierServiceClient::connect(format!("http://{endpoint}")).await?;

        let reason = Reason::NewBlock {
            height: certificate.inner().height(),
            hash: certificate.hash(),
        };
        let request = tonic::Request::new(Notification {
            chain_id: Some(certificate.inner().chain_id().into()),
            reason: bincode::serialize(&reason)?,
        });

        let _response = client.notify(request).await?;
        cancellation_token.cancel();

        let expected_summary = Summary::new(certificate.inner());

        while let Some(summary) = rx.recv().await {
            assert_eq!(summary, expected_summary);
        }

        Ok(())
    }

    #[test_log::test(tokio::test)]
    async fn test_topological_sort() -> anyhow::Result<()> {
        tracing::info!("Starting test: test_topological_sort");
        let port = get_free_port().await?;
        let endpoint = format!("127.0.0.1:{port}");
        let cancellation_token = CancellationToken::new();

        let storage = DbStorage::<MemoryStore, _>::make_test_storage(None).await;
        let service_config = ExporterServiceConfig {
            host: "127.0.0.1".to_string(),
            port,
        };
        let destination_config = DestinationConfig {
            destinations: vec![],
        };

        let context = ExporterContext::new(0, service_config, destination_config);
        let service = ExporterService::from_context(&context, storage.clone()).await?;
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let service = service.with_redirection_buffer(tx);
        tokio::task::spawn(service.run(cancellation_token.clone(), port));
        LocalNet::ensure_grpc_server_has_started("test server", port as usize, "http").await?;
        let client = NotifierServiceClient::connect(format!("http://{endpoint}")).await?;

        let mut protocol_state = ProtocolState::new().populate_protocol_state(4, 10);
        protocol_state
            .synchronize_storage_with_state(storage.clone())
            .await?;

        // test topological sort
        let expected_batch = protocol_state
            .synchronize_block_with_exporter(2, 4, client.clone())
            .await?;
        if let Some(summary) = rx.recv().await {
            assert_eq!(summary, expected_batch.into_summary());
        }

        // test early cutoff
        let expected_batch = protocol_state
            .synchronize_block_with_exporter(2, 6, client.clone())
            .await?;
        if let Some(summary) = rx.recv().await {
            assert_eq!(summary, expected_batch.into_summary());
        }

        cancellation_token.cancel();
        Ok(())
    }
}
