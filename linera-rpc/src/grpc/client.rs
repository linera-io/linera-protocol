// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, future::Future, iter};

use futures::{future, stream, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::BlobContent,
    ensure,
    identifiers::{BlobId, ChainId},
    time::Duration,
};
use linera_chain::{
    data_types::{self},
    types::{
        self, Certificate, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate, Timeout,
        ValidatedBlock,
    },
};
use linera_core::{
    data_types::ChainInfoResponse,
    node::{CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode},
    worker::Notification,
};
use linera_version::VersionInfo;
use tonic::{Code, IntoRequest, Request, Status};
use tracing::{debug, error, info, instrument, warn};
#[cfg(not(web))]
use {
    super::GrpcProtoConversionError,
    crate::{mass_client, RpcMessage},
};

use super::{
    api::{self, validator_node_client::ValidatorNodeClient, SubscriptionRequest},
    transport, GRPC_MAX_MESSAGE_SIZE,
};
use crate::{
    HandleConfirmedCertificateRequest, HandleLiteCertRequest, HandleTimeoutCertificateRequest,
    HandleValidatedCertificateRequest, NodeOptions,
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
        address: String,
        channel: transport::Channel,
        retry_delay: Duration,
        max_retries: u32,
    ) -> Self {
        let client = ValidatorNodeClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);
        Self {
            address,
            client,
            retry_delay,
            max_retries,
        }
    }

    pub fn create(address: String, node_options: NodeOptions) -> Self {
        let options = (&node_options).into();
        let channel = transport::create_channel(address.clone(), &options).unwrap();
        Self::new(
            address,
            channel,
            node_options.retry_delay,
            node_options.max_retries,
        )
    }

    /// Returns whether this gRPC status means the server stream should be reconnected to, or not.
    /// Logs a warning on unexpected status codes.
    fn is_retryable(status: &Status) -> bool {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable | Code::Unknown => {
                info!("gRPC request interrupted: {}; retrying", status);
                true
            }
            Code::Ok | Code::Cancelled | Code::ResourceExhausted => {
                error!("Unexpected gRPC status: {}; retrying", status);
                true
            }
            Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::Internal
            | Code::DataLoss
            | Code::Unauthenticated => {
                error!("Unexpected gRPC status: {}", status);
                false
            }
        }
    }

    async fn delegate<F, Fut, R, S>(
        &self,
        f: F,
        request: impl TryInto<R> + fmt::Debug + Clone,
        handler: &str,
    ) -> Result<S, NodeError>
    where
        F: Fn(ValidatorNodeClient<transport::Channel>, Request<R>) -> Fut,
        Fut: Future<Output = Result<tonic::Response<S>, Status>>,
        R: IntoRequest<R> + Clone,
    {
        debug!(request = ?request, "sending gRPC request");
        let mut retry_count = 0;
        let request_inner = request.try_into().map_err(|_| NodeError::GrpcError {
            error: "could not convert request to proto".to_string(),
        })?;
        loop {
            match f(self.client.clone(), Request::new(request_inner.clone())).await {
                Err(s) if Self::is_retryable(&s) && retry_count < self.max_retries => {
                    let delay = self.retry_delay.saturating_mul(retry_count);
                    retry_count += 1;
                    linera_base::time::timer::sleep(delay).await;
                    continue;
                }
                Err(s) => {
                    return Err(NodeError::GrpcError {
                        error: format!("remote request [{handler}] failed with status: {s:?}",),
                    });
                }
                Ok(result) => return Ok(result.into_inner()),
            };
        }
    }

    #[allow(clippy::result_large_err)]
    fn try_into_chain_info(
        result: api::ChainInfoResult,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let inner = result.inner.ok_or_else(|| NodeError::GrpcError {
            error: "missing body from response".to_string(),
        })?;
        match inner {
            api::chain_info_result::Inner::ChainInfoResponse(response) => {
                Ok(response.try_into().map_err(|err| NodeError::GrpcError {
                    error: format!("failed to unmarshal response: {}", err),
                })?)
            }
            api::chain_info_result::Inner::Error(error) => Err(bincode::deserialize(&error)
                .map_err(|err| NodeError::GrpcError {
                    error: format!("failed to unmarshal error message: {}", err),
                })?),
        }
    }
}

impl TryFrom<api::PendingBlobResult> for BlobContent {
    type Error = NodeError;

    fn try_from(result: api::PendingBlobResult) -> Result<Self, Self::Error> {
        let inner = result.inner.ok_or_else(|| NodeError::GrpcError {
            error: "missing body from response".to_string(),
        })?;
        match inner {
            api::pending_blob_result::Inner::Blob(blob) => {
                Ok(blob.try_into().map_err(|err| NodeError::GrpcError {
                    error: format!("failed to unmarshal response: {}", err),
                })?)
            }
            api::pending_blob_result::Inner::Error(error) => Err(bincode::deserialize(&error)
                .map_err(|err| NodeError::GrpcError {
                    error: format!("failed to unmarshal error message: {}", err),
                })?),
        }
    }
}

macro_rules! client_delegate {
    ($self:ident, $handler:ident, $req:ident) => {{
        $self
            .delegate(
                |mut client, req| async move { client.$handler(req).await },
                $req,
                stringify!($handler),
            )
            .await
    }};
}

impl ValidatorNode for GrpcClient {
    type NotificationStream = NotificationStream;

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_block_proposal(
        &self,
        proposal: data_types::BlockProposal,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_block_proposal, proposal)?)
    }

    #[instrument(target = "grpc_client", skip_all, fields(address = self.address))]
    async fn handle_lite_certificate(
        &self,
        certificate: types::LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleLiteCertRequest {
            certificate,
            wait_for_outgoing_messages,
        };
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_lite_certificate, request)?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_confirmed_certificate(
        &self,
        certificate: GenericCertificate<ConfirmedBlock>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages: bool = delivery.wait_for_outgoing_messages();
        let request = HandleConfirmedCertificateRequest {
            certificate,
            wait_for_outgoing_messages,
        };
        GrpcClient::try_into_chain_info(client_delegate!(
            self,
            handle_confirmed_certificate,
            request
        )?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_validated_certificate(
        &self,
        certificate: GenericCertificate<ValidatedBlock>,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let request = HandleValidatedCertificateRequest { certificate };
        GrpcClient::try_into_chain_info(client_delegate!(
            self,
            handle_validated_certificate,
            request
        )?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_timeout_certificate(
        &self,
        certificate: GenericCertificate<Timeout>,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let request = HandleTimeoutCertificateRequest { certificate };
        GrpcClient::try_into_chain_info(client_delegate!(
            self,
            handle_timeout_certificate,
            request
        )?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn handle_chain_info_query(
        &self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_chain_info_query, query)?)
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
                Option::<Notification>::try_from(result?).map_err(|err| {
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
            .filter_map(|result| {
                future::ready(match result {
                    Ok(notification @ Some(_)) => notification,
                    Ok(None) => None,
                    Err(err) => {
                        warn!("{}", err);
                        None
                    }
                })
            });

        Ok(Box::pin(notification_stream))
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        let req = ();
        Ok(client_delegate!(self, get_version_info, req)?.into())
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError> {
        let req = ();
        Ok(client_delegate!(self, get_genesis_config_hash, req)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err, fields(address = self.address))]
    async fn upload_blob(&self, content: BlobContent) -> Result<BlobId, NodeError> {
        let req = api::BlobContent::try_from(content)?;
        Ok(client_delegate!(self, upload_blob, req)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err, fields(address = self.address))]
    async fn download_blob(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        let req = api::BlobId::try_from(blob_id)?;
        Ok(client_delegate!(self, download_blob, req)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err, fields(address = self.address))]
    async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        let req = api::PendingBlobRequest::try_from((chain_id, blob_id))?;
        client_delegate!(self, download_pending_blob, req)?.try_into()
    }

    #[instrument(target = "grpc_client", skip(self), err, fields(address = self.address))]
    async fn handle_pending_blob(
        &self,
        chain_id: ChainId,
        blob: BlobContent,
    ) -> Result<ChainInfoResponse, NodeError> {
        let req = api::HandlePendingBlobRequest::try_from((chain_id, blob))?;
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_pending_blob, req)?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        ConfirmedBlockCertificate::try_from(Certificate::try_from(client_delegate!(
            self,
            download_certificate,
            hash
        )?)?)
        .map_err(|_| NodeError::UnexpectedCertificateValue)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        let mut missing_hashes = hashes;
        let mut certs_collected = Vec::with_capacity(missing_hashes.len());
        loop {
            // Macro doesn't compile if we pass `missing_hashes.clone()` directly to `client_delegate!`.
            let missing = missing_hashes.clone();
            let mut received: Vec<ConfirmedBlockCertificate> = Vec::<Certificate>::try_from(
                client_delegate!(self, download_certificates, missing)?,
            )?
            .into_iter()
            .map(|cert| {
                ConfirmedBlockCertificate::try_from(cert)
                    .map_err(|_| NodeError::UnexpectedCertificateValue)
            })
            .collect::<Result<_, _>>()?;

            // In the case of the server not returning any certificates, we break the loop.
            if received.is_empty() {
                break;
            }

            // Honest validator should return certificates in the same order as the requested hashes.
            missing_hashes = missing_hashes[received.len()..].to_vec();
            certs_collected.append(&mut received);
        }
        ensure!(
            missing_hashes.is_empty(),
            NodeError::MissingCertificates(missing_hashes)
        );
        Ok(certs_collected)
    }

    #[instrument(target = "grpc_client", skip(self), err, fields(address = self.address))]
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        let req = api::BlobId::try_from(blob_id)?;
        Ok(client_delegate!(self, blob_last_used_by, req)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err, fields(address = self.address))]
    async fn missing_blob_ids(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        let req = api::BlobIds::try_from(blob_ids)?;
        Ok(client_delegate!(self, missing_blob_ids, req)?.try_into()?)
    }
}

#[cfg(not(web))]
#[async_trait::async_trait]
impl mass_client::MassClient for GrpcClient {
    #[instrument(skip_all, err)]
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
                        RpcMessage::TimeoutCertificate(request) => {
                            let request = Request::new((*request).try_into()?);
                            client.handle_timeout_certificate(request).await?
                        }
                        RpcMessage::ValidatedCertificate(request) => {
                            let request = Request::new((*request).try_into()?);
                            client.handle_validated_certificate(request).await?
                        }
                        RpcMessage::ConfirmedCertificate(request) => {
                            let request = Request::new((*request).try_into()?);
                            client.handle_confirmed_certificate(request).await?
                        }
                        msg => panic!("attempted to send msg: {:?}", msg),
                    };
                    match response
                        .into_inner()
                        .inner
                        .ok_or(GrpcProtoConversionError::MissingField)?
                    {
                        api::chain_info_result::Inner::ChainInfoResponse(chain_info_response) => {
                            Ok(Some(RpcMessage::ChainInfoResponse(Box::new(
                                chain_info_response.try_into()?,
                            ))))
                        }
                        api::chain_info_result::Inner::Error(error) => {
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
