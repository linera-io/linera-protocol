// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_trait::async_trait;
use linera_core::worker::Reason;
use linera_rpc::grpc::api::{
    notifier_service_server::{NotifierService, NotifierServiceServer},
    Notification, NotificationBatch,
};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::info;

use crate::common::{get_address, BadNotificationKind, BlockId, ExporterError};

pub(crate) struct ExporterService {
    block_processor_sender: UnboundedSender<BlockId>,
}

#[async_trait]
impl NotifierService for ExporterService {
    async fn notify_batch(
        &self,
        request: Request<NotificationBatch>,
    ) -> Result<Response<()>, Status> {
        for notification in request.into_inner().notifications {
            let block_id =
                match parse_notification(notification).map_err(|e| Status::from_error(e.into())) {
                    Ok(block_id) => {
                        tracing::debug!(
                            ?block_id,
                            "received new block notification from notifier service"
                        );
                        block_id
                    }
                    Err(error) => {
                        // We assume errors when parsing are not critical and just log them.
                        tracing::warn!(?error, "received bad notification");
                        continue;
                    }
                };

            #[cfg(with_metrics)]
            crate::metrics::EXPORTER_NOTIFICATION_QUEUE_LENGTH.inc();
            self.block_processor_sender
                .send(block_id)
                .expect("sender should never fail");
        }

        Ok(Response::new(()))
    }
}

impl ExporterService {
    pub(crate) fn new(sender: UnboundedSender<BlockId>) -> ExporterService {
        ExporterService {
            block_processor_sender: sender,
        }
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

    async fn start_notification_server(
        self,
        port: u16,
        cancellation_token: CancellationToken,
    ) -> core::result::Result<(), ExporterError> {
        let endpoint = get_address(port);
        info!(
            "Starting linera_exporter_service on endpoint = {}",
            endpoint
        );

        let (health_reporter, health_service) = tonic_health::server::health_reporter();
        health_reporter
            .set_serving::<NotifierServiceServer<Self>>()
            .await;

        Server::builder()
            .add_service(health_service)
            .add_service(NotifierServiceServer::new(self))
            .serve_with_shutdown(endpoint, cancellation_token.cancelled_owned())
            .await
            .expect("a running notification server");

        Ok(())
    }
}

fn parse_notification(notification: Notification) -> core::result::Result<BlockId, ExporterError> {
    let chain_id = notification
        .chain_id
        .ok_or(BadNotificationKind::InvalidChainId { inner: None })?
        .try_into()
        .map_err(|err| BadNotificationKind::InvalidChainId { inner: Some(err) })?;

    let reason = bincode::deserialize::<Reason>(&notification.reason)
        .map_err(|err| BadNotificationKind::InvalidReason { inner: Some(err) })?;

    if let Reason::NewBlock { height, hash } = reason {
        return Ok(BlockId::new(chain_id, hash, height));
    }

    Err(BadNotificationKind::InvalidReason { inner: None })?
}

#[cfg(test)]
mod test {
    use linera_base::{crypto::CryptoHash, identifiers::ChainId, port::get_free_port};
    use linera_core::worker::Notification;
    use linera_rpc::grpc::api::notifier_service_client::NotifierServiceClient;
    use linera_service::cli_wrappers::local_net::LocalNet;
    use tokio::sync::mpsc::unbounded_channel;

    use super::*;

    #[test_log::test(tokio::test)]
    async fn test_notification_server() -> anyhow::Result<()> {
        let port = get_free_port().await?;
        let endpoint = format!("127.0.0.1:{port}");
        let cancellation_token = CancellationToken::new();
        let (tx, mut rx) = unbounded_channel();
        let server = ExporterService::new(tx);
        let server_handle = tokio::spawn(server.run(cancellation_token.clone(), port));
        LocalNet::ensure_grpc_server_has_started("test server", port as usize, "http").await?;

        let mut client = NotifierServiceClient::connect(format!("http://{endpoint}")).await?;
        let reason = Reason::NewBlock {
            height: 4.into(),
            hash: CryptoHash::test_hash("s"),
        };
        let request = Notification {
            chain_id: ChainId::default(),
            reason,
        };

        let notification_batch = linera_rpc::grpc::api::NotificationBatch {
            notifications: vec![request.try_into().unwrap()],
        };
        assert!(client
            .notify_batch(Request::new(notification_batch))
            .await
            .is_ok());

        let expected_block_id =
            BlockId::new(ChainId::default(), CryptoHash::test_hash("s"), 4.into());
        assert!(rx
            .recv()
            .await
            .is_some_and(|block_id| block_id == expected_block_id));

        cancellation_token.cancel();
        assert!(server_handle.await.is_ok());

        Ok(())
    }
}
