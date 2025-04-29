// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::net::SocketAddr;

use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId, listen_for_shutdown_signals,
};
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

use crate::{state::BlockExporterStateView, ExporterError};

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

        {
            let mut guard = self.state.lock().await;
            guard
                .initialize_chain(&chain_id, (block_height, block_hash))
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            guard
                .save()
                .await
                .map_err(|e| Status::from_error(e.into()))?;
        }

        // after implementation of future destinations
        // this will be offloaded to a seperate thread.
        #[cfg(with_testing)]
        {
            let block = self
                .storage
                .read_confirmed_block(block_hash)
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            let byte_size = bcs::serialized_size(&block).unwrap();
            let summary = Summary {
                chain_id,
                block_height,
                block_hash,
                byte_size,
            };

            if cfg!(feature = "test") {
                tracing::debug!("Block exporter batch summary: {:?}", summary);
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
    byte_size: usize,
}

#[cfg(with_testing)]
impl Summary {
    pub fn new(
        chain_id: ChainId,
        block_height: BlockHeight,
        block_hash: CryptoHash,
        byte_size: usize,
    ) -> Summary {
        Summary {
            chain_id,
            block_height,
            block_hash,
            byte_size,
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use linera_base::{
        crypto::CryptoHash,
        data_types::{Amount, Round},
        port::get_free_port,
    };
    use linera_chain::{
        data_types::{BlockExecutionOutcome, OperationResult},
        test::{make_first_block, BlockTestExt},
        types::{ConfirmedBlock, ConfirmedBlockCertificate},
    };
    use linera_rpc::grpc::api::notifier_service_client::NotifierServiceClient;
    use linera_service::cli_wrappers::local_net::LocalNet;
    use linera_storage::DbStorage;
    use linera_views::memory::MemoryStore;

    use super::*;

    #[tokio::test]
    async fn test_notification_service() -> anyhow::Result<()> {
        linera_base::tracing::init("linera-exporter");
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

        let dummy_chain_id = ChainId(CryptoHash::test_hash("root1"));

        let block = BlockExecutionOutcome {
            messages: vec![Vec::new()],
            previous_message_blocks: BTreeMap::new(),
            state_hash: CryptoHash::test_hash("state"),
            oracle_responses: vec![Vec::new()],
            events: vec![Vec::new()],
            blobs: vec![Vec::new()],
            operation_results: vec![OperationResult::default()],
        }
        .with(make_first_block(dummy_chain_id).with_simple_transfer(dummy_chain_id, Amount::ONE));
        let confirmed_block = ConfirmedBlock::new(block);
        let expected_byte_size = bcs::serialized_size(&confirmed_block).unwrap(); // here just to avoid cloning after the move below.
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

        let expected_chain_id = certificate.inner().chain_id();
        let expected_block_height = certificate.inner().height();
        let expected_block_hash = certificate.hash();
        let expected_summary = Summary::new(
            expected_chain_id,
            expected_block_height,
            expected_block_hash,
            expected_byte_size,
        );

        while let Some(summary) = rx.recv().await {
            assert_eq!(summary, expected_summary);
        }

        Ok(())
    }
}
