// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use linera_base::{
    crypto::CryptoHash, data_types::BlockHeight, identifiers::ChainId, listen_for_shutdown_signals,
};
use linera_client::config::{DestinationConfig, ServiceConfig};
use linera_core::worker::Reason;
use linera_rpc::grpc::api::{
    notifier_service_server::{NotifierService, NotifierServiceServer},
    Notification,
};
use linera_sdk::views::RootView;
use linera_views::{context::ViewContext, store::KeyValueStore};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

use crate::{state::BlockExporterStateView, storage::ExporterStorage, ExporterError, Generic};

#[derive(Debug)]
pub(super) struct ExporterContext {
    service_config: ServiceConfig,
    destination_config: DestinationConfig,
}

pub(crate) struct ExporterService<Store> {
    // For now a Mutex should suffice as a simple workaround.
    // As everything is currently happening on the single master thread.
    // No tasks on the worker threads have been spawned yet.
    state: Mutex<BlockExporterStateView<ViewContext<(), Store>>>,
    destination_config: DestinationConfig,
    storage: ExporterStorage<Store>,
}

#[async_trait]
impl<S> NotifierService for ExporterService<S>
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: Send + Sync,
{
    async fn notify(&self, request: Request<Notification>) -> Result<Response<()>, Status> {
        let notification = request.into_inner();
        let (chain_id, height, hash) =
            parse_notification(notification).map_err(|e| Status::from_error(e.into()))?;

        {
            let mut guard = self.state.lock().await;
            guard
                .initialize_chain(&chain_id, (height, hash))
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            guard
                .save()
                .await
                .map_err(|e| Status::from_error(e.into()))?;
        }

        info!("Just recieved a notification");

        // Since the rest of the module is in an unimplimented state, the only way to
        // test, was to either hard code the test like this
        // or create a new service endpoint in a new proto file or in the linera-rpc crate.
        #[cfg(test)]
        {
            let guard = self.state.lock().await;
            let tip = guard
                .get_chain_tip(&chain_id)
                .await
                .map_err(|e| Status::from_error(e.into()))?;
            assert!(tip.is_some_and(|updated_height| updated_height == height));
            info!("height is: {height}");
        }

        // only required in integeration
        // where a worker continuously writes to a shared storage
        #[cfg(not(test))]
        if self.destination_config.debug_mode {
            let block = self
                .storage
                .read_confirmed_block(hash)
                .await
                .map_err(ExporterError::StorageError)
                .map_err(|e| Status::from_error(e.into()))?;
            let hash = block.into_inner().hash();
            println!("{hash}");
        }

        Ok(Response::new(()))
    }
}

impl<S> ExporterService<S>
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: Send + Sync,
{
    async fn from_context(
        context: &ExporterContext,
        storage: ExporterStorage<S>,
    ) -> core::result::Result<Self, ExporterError> {
        let state = storage
            .load_exporter_state()
            .await
            .map_err(ExporterError::StateError)?;
        let destination_config = context.destination_config.clone();
        Ok(Self {
            state: Mutex::new(state),
            destination_config,
            storage,
        })
    }

    pub async fn run(
        self,
        canellation_token: CancellationToken,
        endpoint: String,
    ) -> core::result::Result<(), ExporterError> {
        info!("Linera exporter is running.");
        self.start_notification_server(endpoint, canellation_token)
            .await
    }
}

impl ExporterContext {
    pub(super) async fn run<S>(
        self,
        storage: ExporterStorage<S>,
    ) -> core::result::Result<(), ExporterError>
    where
        S: KeyValueStore + Clone + Send + Sync + 'static,
        S::Error: Send + Sync,
    {
        let shutdown_notifier = CancellationToken::new();
        tokio::spawn(listen_for_shutdown_signals(shutdown_notifier.clone()));

        let endpoint = format!("{}:{}", self.service_config.host, self.service_config.port);
        let service = ExporterService::from_context(&self, storage).await?;
        service.run(shutdown_notifier, endpoint).await
    }
}

impl ExporterContext {
    pub(super) fn from_config(
        service_config: ServiceConfig,
        destination_config: DestinationConfig,
    ) -> ExporterContext {
        Self {
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

impl<S> ExporterService<S>
where
    S: KeyValueStore + Clone + Send + Sync + 'static,
    S::Error: Send + Sync,
{
    pub async fn start_notification_server(
        self,
        endpoint: String,
        canellation_token: CancellationToken,
    ) -> core::result::Result<(), ExporterError> {
        let endpoint = endpoint.parse().into_unknown()?;
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
            .serve_with_shutdown(endpoint, canellation_token.cancelled_owned())
            .await
            .expect("a running notification server");

        if cfg!(feature = "test") {
            Ok(())
        } else {
            Err(ExporterError::GenericError(
                "Service should run forever.".to_string().into(),
            ))
        }
    }
}

#[cfg(test)]
mod test {
    use linera_base::{crypto::CryptoHash, port::get_free_port};
    use linera_rpc::grpc::api::notifier_service_client::NotifierServiceClient;
    use linera_service::cli_wrappers::local_net::LocalNet;
    use linera_views::memory::MemoryStore;

    use super::*;

    #[tokio::test]
    async fn test_notification_service() -> Result<()> {
        linera_base::tracing::init("linera-exporter");
        let port = get_free_port().await?;
        let endpoint = format!("127.0.0.1:{port}");

        let canellation_token = CancellationToken::new();

        let storage = ExporterStorage::<MemoryStore>::make_test_storage().await;
        let service_config = ServiceConfig {
            host: "127.0.0.1".to_string(),
            port,
        };
        let destination_config = DestinationConfig {
            destinations: vec![],
            debug_mode: true,
        };
        let context = ExporterContext::from_config(service_config, destination_config);
        let service = ExporterService::from_context(&context, storage).await?;
        tokio::spawn(service.run(canellation_token.clone(), endpoint.clone()));

        LocalNet::ensure_grpc_server_has_started("test server", port as usize, "http").await?;

        let mut client = NotifierServiceClient::connect(format!("http://{endpoint}")).await?;

        let reason = Reason::NewBlock {
            height: 46.into(),
            hash: CryptoHash::test_hash("0"),
        };
        let request = tonic::Request::new(Notification {
            chain_id: Some(ChainId(CryptoHash::test_hash("abc")).into()),
            reason: bincode::serialize(&reason)?,
        });

        let _response = client.notify(request).await?;
        canellation_token.cancel();

        Ok(())
    }
}
