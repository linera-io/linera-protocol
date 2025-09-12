// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, future::Future, iter, sync::Arc};

use futures::{future, stream, StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{BlobContent, BlockHeight, NetworkDescription},
    ensure,
    identifiers::{BlobId, ChainId},
    time::Duration,
};
use linera_chain::{
    data_types::{self},
    types::{
        self, Certificate, ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate,
        LiteCertificate, Timeout, ValidatedBlock,
    },
};
use linera_core::{
    data_types::{CertificatesByHeightRequest, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode},
    worker::Notification,
};
use linera_version::VersionInfo;
use tonic::{Code, IntoRequest, Request, Status};
use tracing::{debug, info, instrument, warn, Level};

use super::{
    api::{self, validator_node_client::ValidatorNodeClient, SubscriptionRequest},
    pool::GrpcConnectionPool,
    transport, GRPC_MAX_MESSAGE_SIZE,
};
use crate::{
    grpc::api::RawCertificate, HandleConfirmedCertificateRequest, HandleLiteCertRequest,
    HandleTimeoutCertificateRequest, HandleValidatedCertificateRequest,
};

#[derive(Clone)]
pub struct GrpcClient {
    address: String,
    pool: Arc<GrpcConnectionPool>,
    retry_delay: Duration,
    max_retries: u32,
}

impl GrpcClient {
    pub fn new(
        address: String,
        pool: Arc<GrpcConnectionPool>,
        retry_delay: Duration,
        max_retries: u32,
    ) -> Result<Self, super::GrpcError> {
        // Just verify we can get a channel to this address
        let _ = pool.channel(address.clone())?;
        Ok(Self {
            address,
            pool,
            retry_delay,
            max_retries,
        })
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    fn make_client(&self) -> Result<ValidatorNodeClient<transport::Channel>, super::GrpcError> {
        let channel = self.pool.channel(self.address.clone())?;
        Ok(ValidatorNodeClient::new(channel)
            .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
            .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE))
    }

    /// Returns whether this gRPC status means the server stream should be reconnected to, or not.
    /// Logs a warning on unexpected status codes.
    fn is_retryable_needs_reconnect(status: &Status) -> (bool, bool) {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable | Code::Unknown => {
                info!("gRPC request interrupted: {}; retrying", status);
                (true, false)
            }
            Code::Ok | Code::Cancelled | Code::ResourceExhausted => {
                info!("Unexpected gRPC status: {}; retrying", status);
                (true, false)
            }
            Code::NotFound => (false, false), // This code is used if e.g. the validator is missing blobs.
            Code::Internal => {
                let error_string = status.to_string();
                if error_string.contains("GoAway") && error_string.contains("max_age") {
                    info!(
                        "gRPC connection hit max_age and got a GoAway: {}; reconnecting then retrying",
                        status
                    );
                    return (true, true);
                }
                info!("Unexpected gRPC status: {}", status);
                (false, false)
            }
            Code::InvalidArgument
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::DataLoss
            | Code::Unauthenticated => {
                info!("Unexpected gRPC status: {}", status);
                (false, false)
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
        let mut retry_count = 0;
        let request_inner = request.try_into().map_err(|_| NodeError::GrpcError {
            error: "could not convert request to proto".to_string(),
        })?;

        let mut reconnected = false;
        loop {
            // Create client on-demand for each attempt
            let client = match self.make_client() {
                Ok(client) => client,
                Err(e) => {
                    return Err(NodeError::GrpcError {
                        error: format!("Failed to create client: {}", e),
                    });
                }
            };

            match f(client, Request::new(request_inner.clone())).await {
                Err(s) => {
                    let (is_retryable, needs_reconnect) = Self::is_retryable_needs_reconnect(&s);
                    if is_retryable && retry_count < self.max_retries {
                        // If this error indicates we need a connection refresh and we haven't already tried, do it
                        if needs_reconnect && !reconnected {
                            info!("Connection error detected, invalidating channel: {}", s);
                            self.pool.invalidate_channel(&self.address);
                            reconnected = true;
                        }

                        let delay = self.retry_delay.saturating_mul(retry_count);
                        retry_count += 1;
                        linera_base::time::timer::sleep(delay).await;
                        continue;
                    }

                    return Err(NodeError::GrpcError {
                        error: format!("remote request [{handler}] failed with status: {s:?}"),
                    });
                }
                Ok(result) => return Ok(result.into_inner()),
            };
        }
    }

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
        debug!(
            handler = stringify!($handler),
            request = ?$req,
            "sending gRPC request"
        );
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
    async fn handle_chain_info_query(
        &self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_chain_info_query, query)?)
    }

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
    async fn subscribe(&self, chains: Vec<ChainId>) -> Result<Self::NotificationStream, NodeError> {
        let retry_delay = self.retry_delay;
        let max_retries = self.max_retries;
        let mut retry_count = 0;
        let subscription_request = SubscriptionRequest {
            chain_ids: chains.into_iter().map(|chain| chain.into()).collect(),
        };
        let pool = self.pool.clone();
        let address = self.address.clone();

        // Make the first connection attempt before returning from this method.
        let mut stream = Some({
            let mut client = self
                .make_client()
                .map_err(|e| NodeError::SubscriptionFailed {
                    status: format!("Failed to create client: {}", e),
                })?;
            client
                .subscribe(subscription_request.clone())
                .await
                .map_err(|status| NodeError::SubscriptionFailed {
                    status: status.to_string(),
                })?
                .into_inner()
        });

        // A stream of `Result<grpc::Notification, tonic::Status>` that keeps calling
        // `client.subscribe(request)` endlessly and without delay.
        let endlessly_retrying_notification_stream = stream::unfold((), move |()| {
            let pool = pool.clone();
            let address = address.clone();
            let subscription_request = subscription_request.clone();
            let mut stream = stream.take();
            async move {
                let stream = if let Some(stream) = stream.take() {
                    future::Either::Right(stream)
                } else {
                    // Create a new client for each reconnection attempt
                    match pool.channel(address.clone()) {
                        Ok(channel) => {
                            let mut client = ValidatorNodeClient::new(channel)
                                .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                                .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE);
                            match client.subscribe(subscription_request.clone()).await {
                                Err(err) => {
                                    future::Either::Left(stream::iter(iter::once(Err(err))))
                                }
                                Ok(response) => future::Either::Right(response.into_inner()),
                            }
                        }
                        Err(e) => {
                            let status = tonic::Status::unavailable(format!(
                                "Failed to create channel: {}",
                                e
                            ));
                            future::Either::Left(stream::iter(iter::once(Err(status))))
                        }
                    }
                };
                Some((stream, ()))
            }
        })
        .flatten();

        let span = tracing::info_span!("notification stream");
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

                let (is_retryable, _) =
                    span.in_scope(|| Self::is_retryable_needs_reconnect(status));
                if !is_retryable || retry_count >= max_retries {
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        let req = ();
        Ok(client_delegate!(self, get_version_info, req)?.into())
    }

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
    async fn get_network_description(&self) -> Result<NetworkDescription, NodeError> {
        let req = ();
        Ok(client_delegate!(self, get_network_description, req)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn upload_blob(&self, content: BlobContent) -> Result<BlobId, NodeError> {
        Ok(client_delegate!(self, upload_blob, content)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn download_blob(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        Ok(client_delegate!(self, download_blob, blob_id)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        let req = (chain_id, blob_id);
        client_delegate!(self, download_pending_blob, req)?.try_into()
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn handle_pending_blob(
        &self,
        chain_id: ChainId,
        blob: BlobContent,
    ) -> Result<ChainInfoResponse, NodeError> {
        let req = (chain_id, blob);
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_pending_blob, req)?)
    }

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::WARN), fields(address = self.address))]
    async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        let mut missing_hashes = hashes;
        let mut certs_collected = Vec::with_capacity(missing_hashes.len());
        while !missing_hashes.is_empty() {
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

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn download_certificates_by_heights(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError> {
        let mut missing = heights;
        let mut certs_collected = vec![];
        while !missing.is_empty() {
            let request = CertificatesByHeightRequest {
                chain_id,
                heights: missing.clone(),
            };
            let mut received: Vec<ConfirmedBlockCertificate> =
                client_delegate!(self, download_raw_certificates_by_heights, request)?
                    .certificates
                    .into_iter()
                    .map(
                        |RawCertificate {
                             lite_certificate,
                             confirmed_block,
                         }| {
                            let cert = bcs::from_bytes::<LiteCertificate>(&lite_certificate)
                                .map_err(|_| NodeError::UnexpectedCertificateValue)?;

                            let block = bcs::from_bytes::<ConfirmedBlock>(&confirmed_block)
                                .map_err(|_| NodeError::UnexpectedCertificateValue)?;

                            cert.with_value(block)
                                .ok_or(NodeError::UnexpectedCertificateValue)
                        },
                    )
                    .collect::<Result<_, _>>()?;

            if received.is_empty() {
                break;
            }

            // Honest validator should return certificates in the same order as the requested hashes.
            missing = missing[received.len()..].to_vec();
            certs_collected.append(&mut received);
        }

        Ok(certs_collected)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        Ok(client_delegate!(self, blob_last_used_by, blob_id)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn blob_last_used_by_certificate(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        Ok(client_delegate!(self, blob_last_used_by_certificate, blob_id)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn missing_blob_ids(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        Ok(client_delegate!(self, missing_blob_ids, blob_ids)?.try_into()?)
    }
}
