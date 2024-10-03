// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::iter;

use futures::{future, stream, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlobContent},
    identifiers::{BlobId, ChainId},
    time::Duration,
};
use linera_chain::data_types::{self, Certificate, CertificateValue, HashedCertificateValue};
use linera_core::{
    node::{CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode},
    worker::Notification,
};
use linera_version::VersionInfo;
use tonic::{Code, Request, Status};
use tracing::{debug, info, instrument, warn};
#[cfg(not(web))]
use {
    super::GrpcProtoConversionError,
    crate::{mass_client, RpcMessage},
};

use super::{
    api::{
        self, chain_info_result::Inner, validator_node_client::ValidatorNodeClient,
        SubscriptionRequest,
    },
    transport, GrpcError, GRPC_MAX_MESSAGE_SIZE,
};
use crate::{
    config::ValidatorPublicNetworkConfig, node_provider::NodeOptions, HandleCertificateRequest,
    HandleLiteCertRequest,
};

#[derive(Clone)]
pub struct GrpcClient {
    address: String,
    client: ValidatorNodeClient<transport::Channel>,
    retry_delay: Duration,
    max_retries: u32,
}

impl GrpcClient {
    pub fn new(
        network: ValidatorPublicNetworkConfig,
        options: NodeOptions,
    ) -> Result<Self, GrpcError> {
        let address = network.http_address();

        let channel =
            transport::create_channel(address.clone(), &transport::Options::from(&options))?;
        let client = ValidatorNodeClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);

        Ok(Self {
            address,
            client,
            retry_delay: options.retry_delay,
            max_retries: options.max_retries,
        })
    }

    /// Returns whether this gRPC status means the server stream should be reconnected to, or not.
    /// Logs a warning on unexpected status codes.
    fn is_retryable(status: &Status) -> bool {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable | Code::Unknown => {
                info!("Notification stream interrupted: {}; retrying", status);
                true
            }
            Code::Ok
            | Code::Cancelled
            | Code::NotFound
            | Code::AlreadyExists
            | Code::ResourceExhausted => {
                warn!("Unexpected gRPC status: {}; retrying", status);
                true
            }
            Code::InvalidArgument
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::Internal
            | Code::DataLoss
            | Code::Unauthenticated => {
                warn!("Unexpected gRPC status: {}", status);
                false
            }
        }
    }
}

macro_rules! client_delegate {
    ($self:ident, $handler:ident, $req:ident) => {{
        debug!(request = ?$req, "sending gRPC request");
        let mut retry_count = 0;
        loop {
            let request_inner = $req.clone().try_into().map_err(|_| NodeError::GrpcError {
                error: "could not convert request to proto".to_string(),
            })?;
            let request = Request::new(request_inner);
            let inner = match $self.client.clone().$handler(request).await {
                Err(s) if Self::is_retryable(&s)
                    && retry_count < $self.max_retries =>
                {
                    let delay = $self.retry_delay.saturating_mul(retry_count);
                    retry_count += 1;
                    linera_base::time::timer::sleep(delay).await;
                    continue;
                }
                Err(s) => {
                    return Err(NodeError::GrpcError {
                        error: format!(
                            "remote request [{}] failed with status: {s:?}",
                            stringify!($handler),
                        ),
                    });
                }
                Ok(result) => {
                    result.into_inner().inner.ok_or(NodeError::GrpcError {
                        error: "missing body from response".to_string(),
                    })?
                }
            };
            return match inner {
                Inner::ChainInfoResponse(response) => {
                    Ok(response.try_into().map_err(|err| NodeError::GrpcError {
                        error: format!("failed to marshal response: {}", err),
                    })?)
                }
                Inner::Error(error) => {
                    Err(bincode::deserialize(&error).map_err(|err| NodeError::GrpcError {
                        error: format!("failed to marshal error message: {}", err),
                    })?)
                }
            };
        }
    }};
}

impl ValidatorNode for GrpcClient {
    type NotificationStream = NotificationStream;

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_block_proposal(
        &self,
        proposal: data_types::BlockProposal,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_block_proposal, proposal)
    }

    #[instrument(target = "grpc_client", skip_all, fields(address = self.address))]
    async fn handle_lite_certificate(
        &self,
        certificate: data_types::LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleLiteCertRequest {
            certificate,
            wait_for_outgoing_messages,
        };
        client_delegate!(self, handle_lite_certificate, request)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleCertificateRequest {
            certificate,
            blobs,
            wait_for_outgoing_messages,
        };
        client_delegate!(self, handle_certificate, request)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_chain_info_query(
        &self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        client_delegate!(self, handle_chain_info_query, query)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn subscribe(&self, chains: Vec<ChainId>) -> Result<Self::NotificationStream, NodeError> {
        let retry_delay = self.retry_delay;
        let max_retries = self.max_retries;
        let mut retry_count = 0;
        let subscription_request = SubscriptionRequest {
            chain_ids: chains.into_iter().map(|chain| chain.into()).collect(),
        };
        let mut client = self.client.clone();

        // Make the first connection attempt before returning from this method.
        let mut stream = Some(
            client
                .subscribe(subscription_request.clone())
                .await
                .map_err(|status| NodeError::SubscriptionFailed {
                    status: status.to_string(),
                })?
                .into_inner(),
        );

        // A stream of `Result<grpc::Notification, tonic::Status>` that keeps calling
        // `client.subscribe(request)` endlessly and without delay.
        let endlessly_retrying_notification_stream = stream::unfold((), move |()| {
            let mut client = client.clone();
            let subscription_request = subscription_request.clone();
            let mut stream = stream.take();
            async move {
                let stream = if let Some(stream) = stream.take() {
                    future::Either::Right(stream)
                } else {
                    match client.subscribe(subscription_request.clone()).await {
                        Err(err) => future::Either::Left(stream::iter(iter::once(Err(err)))),
                        Ok(response) => future::Either::Right(response.into_inner()),
                    }
                };
                Some((stream, ()))
            }
        })
        .flatten();

        // The stream of `Notification`s that inserts increasing delays after retriable errors, and
        // terminates after unexpected or fatal errors.
        let notification_stream = endlessly_retrying_notification_stream
            .map(|result| {
                Notification::try_from(result?).map_err(|err| {
                    let message = format!("Could not deserialize notification: {}", err);
                    tonic::Status::new(Code::Internal, message)
                })
            })
            .take_while(move |result| {
                let Err(status) = result else {
                    retry_count = 0;
                    return future::Either::Left(future::ready(true));
                };
                if !Self::is_retryable(status) || retry_count >= max_retries {
                    return future::Either::Left(future::ready(false));
                }
                let delay = retry_delay.saturating_mul(retry_count);
                retry_count += 1;
                future::Either::Right(async move {
                    linera_base::time::timer::sleep(delay).await;
                    true
                })
            })
            .filter_map(|result| future::ready(result.ok()));

        Ok(Box::pin(notification_stream))
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        Ok(self
            .client
            .clone()
            .get_version_info(())
            .await?
            .into_inner()
            .into())
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError> {
        Ok(self
            .client
            .clone()
            .get_genesis_config_hash(())
            .await?
            .into_inner()
            .try_into()?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_blob_content(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        Ok(self
            .client
            .clone()
            .download_blob_content(api::BlobId::try_from(blob_id)?)
            .await?
            .into_inner()
            .try_into()?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, NodeError> {
        let certificate_value: CertificateValue = self
            .client
            .clone()
            .download_certificate_value(<CryptoHash as Into<api::CryptoHash>>::into(hash))
            .await?
            .into_inner()
            .try_into()?;
        Ok(certificate_value.with_hash_checked(hash)?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificate(&self, hash: CryptoHash) -> Result<Certificate, NodeError> {
        Ok(self
            .client
            .clone()
            .download_certificate(<CryptoHash as Into<api::CryptoHash>>::into(hash))
            .await?
            .into_inner()
            .try_into()?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        Ok(self
            .client
            .clone()
            .blob_last_used_by(api::BlobId::try_from(blob_id)?)
            .await?
            .into_inner()
            .try_into()?)
    }
}

#[cfg(not(web))]
#[async_trait::async_trait]
impl mass_client::MassClient for GrpcClient {
    #[tracing::instrument(skip_all, err)]
    async fn send(
        &self,
        requests: Vec<RpcMessage>,
        max_in_flight: usize,
    ) -> Result<Vec<RpcMessage>, mass_client::MassClientError> {
        let client = self.client.clone();
        let responses = stream::iter(requests)
            .map(|request| {
                let mut client = client.clone();
                async move {
                    let response = match request {
                        RpcMessage::BlockProposal(proposal) => {
                            let request = Request::new((*proposal).try_into()?);
                            client.handle_block_proposal(request).await?
                        }
                        RpcMessage::Certificate(request) => {
                            let request = Request::new((*request).try_into()?);
                            client.handle_certificate(request).await?
                        }
                        msg => panic!("attempted to send msg: {:?}", msg),
                    };
                    match response
                        .into_inner()
                        .inner
                        .ok_or(GrpcProtoConversionError::MissingField)?
                    {
                        Inner::ChainInfoResponse(chain_info_response) => {
                            Ok(Some(RpcMessage::ChainInfoResponse(Box::new(
                                chain_info_response.try_into()?,
                            ))))
                        }
                        Inner::Error(error) => {
                            let error = bincode::deserialize::<NodeError>(&error)
                                .map_err(GrpcProtoConversionError::BincodeError)?;
                            tracing::error!(?error, "received error response");
                            Ok(None)
                        }
                    }
                }
            })
            .buffer_unordered(max_in_flight)
            .filter_map(
                |result: Result<Option<_>, mass_client::MassClientError>| async move {
                    result.transpose()
                },
            )
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
        Ok(responses)
    }
}
