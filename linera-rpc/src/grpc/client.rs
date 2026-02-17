// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    collections::BTreeSet,
    fmt,
    future::Future,
    iter,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};

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
use tracing::{debug, instrument, trace, warn, Level};

use super::{
    api::{self, validator_node_client::ValidatorNodeClient, SubscriptionRequest},
    transport, GRPC_MAX_MESSAGE_SIZE,
};
#[cfg(feature = "opentelemetry")]
use crate::propagation::{get_context_with_traffic_type, inject_context};
use crate::{
    grpc::api::RawCertificate, HandleConfirmedCertificateRequest, HandleLiteCertRequest,
    HandleTimeoutCertificateRequest, HandleValidatedCertificateRequest,
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

    pub fn address(&self) -> &str {
        &self.address
    }

    /// Returns whether this gRPC status means the server stream should be reconnected to, or not.
    /// Logs a warning on unexpected status codes.
    fn is_retryable(status: &Status) -> bool {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable | Code::Unknown => {
                trace!("gRPC request interrupted: {status:?}; retrying");
                true
            }
            Code::Ok | Code::Cancelled | Code::ResourceExhausted => {
                trace!("Unexpected gRPC status: {status:?}; retrying");
                true
            }
            Code::Internal if status.message().contains("h2 protocol error") => {
                // HTTP/2 connection reset errors are transient network issues, not real
                // internal errors. This happens when the server restarts and the
                // connection is forcibly closed.
                trace!("gRPC connection reset: {status:?}; retrying");
                true
            }
            Code::NotFound => false, // This code is used if e.g. the validator is missing blobs.
            Code::InvalidArgument
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::Internal
            | Code::DataLoss
            | Code::Unauthenticated => {
                trace!("Unexpected gRPC status: {status:?}");
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
        let mut retry_count = 0;
        let request_inner = request.try_into().map_err(|_| NodeError::GrpcError {
            error: "could not convert request to proto".to_string(),
        })?;
        loop {
            #[allow(unused_mut)]
            let mut request = Request::new(request_inner.clone());
            // Inject OpenTelemetry context (trace context + baggage) into gRPC metadata.
            // This uses get_context_with_traffic_type() to also check the LINERA_TRAFFIC_TYPE
            // environment variable, allowing benchmark tools to mark their traffic as synthetic.
            #[cfg(feature = "opentelemetry")]
            inject_context(&get_context_with_traffic_type(), request.metadata_mut());
            match f(self.client.clone(), request).await {
                Err(s) if Self::is_retryable(&s) && retry_count < self.max_retries => {
                    let delay = self.retry_delay.saturating_mul(retry_count);
                    retry_count += 1;
                    linera_base::time::timer::sleep(delay).await;
                    continue;
                }
                Err(s) => {
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

    fn address(&self) -> String {
        self.address.clone()
    }

    #[instrument(target = "grpc_client", skip_all, err(level = Level::DEBUG), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::DEBUG), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::DEBUG), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::DEBUG), fields(address = self.address))]
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

    #[instrument(target = "grpc_client", skip_all, err(level = Level::DEBUG), fields(address = self.address))]
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
        // Use shared atomic counter so unfold can reset it on successful reconnection.
        let retry_count = Arc::new(AtomicU32::new(0));
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
        let retry_count_for_unfold = retry_count.clone();
        let endlessly_retrying_notification_stream = stream::unfold((), move |()| {
            let mut client = client.clone();
            let subscription_request = subscription_request.clone();
            let mut stream = stream.take();
            let retry_count = retry_count_for_unfold.clone();
            async move {
                let stream = if let Some(stream) = stream.take() {
                    future::Either::Right(stream)
                } else {
                    match client.subscribe(subscription_request.clone()).await {
                        Err(err) => future::Either::Left(stream::iter(iter::once(Err(err)))),
                        Ok(response) => {
                            // Reset retry count on successful reconnection.
                            retry_count.store(0, Ordering::Relaxed);
                            trace!("Successfully reconnected subscription stream");
                            future::Either::Right(response.into_inner())
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
                    retry_count.store(0, Ordering::Relaxed);
                    return future::Either::Left(future::ready(true));
                };

                let current_retry_count = retry_count.load(Ordering::Relaxed);
                if !span.in_scope(|| Self::is_retryable(status))
                    || current_retry_count >= max_retries
                {
                    return future::Either::Left(future::ready(false));
                }
                let delay = retry_delay.saturating_mul(current_retry_count);
                retry_count.fetch_add(1, Ordering::Relaxed);
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
        let mut missing: BTreeSet<BlockHeight> = heights.into_iter().collect();
        let mut certs_collected = vec![];
        while !missing.is_empty() {
            let request = CertificatesByHeightRequest {
                chain_id,
                heights: missing.iter().copied().collect(),
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

            // Remove only the heights we actually received from missing set.
            for cert in &received {
                missing.remove(&cert.inner().height());
            }
            certs_collected.append(&mut received);
        }
        certs_collected.sort_by_key(|cert| cert.inner().height());
        Ok(certs_collected)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        Ok(client_delegate!(self, blob_last_used_by, blob_id)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn missing_blob_ids(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        Ok(client_delegate!(self, missing_blob_ids, blob_ids)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip(self), err(level = Level::WARN), fields(address = self.address))]
    async fn blob_last_used_by_certificate(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError> {
        Ok(client_delegate!(self, blob_last_used_by_certificate, blob_id)?.try_into()?)
    }
}
