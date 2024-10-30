// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{fmt, future::Future, iter};

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
use tonic::{Code, IntoRequest, Request, Status};
use tracing::{debug, error, info, instrument, warn};
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
    fn is_retryable(status: &Status, address: &str) -> bool {
        match status.code() {
            Code::DeadlineExceeded | Code::Aborted | Code::Unavailable | Code::Unknown => {
                info!("gRPC request to {address} interrupted: {status}; retrying");
                true
            }
            Code::Ok
            | Code::Cancelled
            | Code::NotFound
            | Code::AlreadyExists
            | Code::ResourceExhausted => {
                error!("gRPC request to {address} interrupted: {status}; retrying");
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
                error!("Unexpected gRPC status received from {address}: {status}");
                false
            }
        }
    }

    async fn delegate<F, Fut, R, S>(
        &self,
        f: F,
        request: impl TryInto<R> + fmt::Debug + Clone,
        handler: &str,
        address: &str,
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
                Err(s) if Self::is_retryable(&s, address) && retry_count < self.max_retries => {
                    let delay = self.retry_delay.saturating_mul(retry_count);
                    retry_count += 1;
                    linera_base::time::timer::sleep(delay).await;
                    continue;
                }
                Err(s) => {
                    return Err(NodeError::GrpcError {
                        error: format!(
                            "remote request [{handler}] to {address} failed with status: {s:?}",
                        ),
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
        let inner = result.inner.ok_or(NodeError::GrpcError {
            error: "missing body from response".to_string(),
        })?;
        match inner {
            Inner::ChainInfoResponse(response) => {
                Ok(response.try_into().map_err(|err| NodeError::GrpcError {
                    error: format!("failed to unmarshal response: {}", err),
                })?)
            }
            Inner::Error(error) => {
                Err(
                    bincode::deserialize(&error).map_err(|err| NodeError::GrpcError {
                        error: format!("failed to unmarshal error message: {}", err),
                    })?,
                )
            }
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
                &$self.address,
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
        certificate: data_types::LiteCertificate<'_>,
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
        GrpcClient::try_into_chain_info(client_delegate!(self, handle_certificate, request)?)
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
        let address = self.address.clone();
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
                if !Self::is_retryable(status, &address) || retry_count >= max_retries {
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

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_blob_content(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        let req = api::BlobId::try_from(blob_id)?;
        Ok(client_delegate!(self, download_blob_content, req)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, NodeError> {
        let value = client_delegate!(self, download_certificate_value, hash)?;
        Ok(CertificateValue::try_from(value)?.with_hash_checked(hash)?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificate(&self, hash: CryptoHash) -> Result<Certificate, NodeError> {
        Ok(client_delegate!(self, download_certificate, hash)?.try_into()?)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<Certificate>, NodeError> {
        let mut missing_hashes = hashes;
        let mut certs_collected = Vec::with_capacity(missing_hashes.len());
        loop {
            // Macro doesn't compile if we pass `missing_hashes.clone()` directly to `client_delegate!`.
            let missing = missing_hashes.clone();
            let mut received: Vec<Certificate> =
                client_delegate!(self, download_certificates, missing)?.try_into()?;

            // In the case of the server not returning any certificates, we break the loop.
            if received.is_empty() {
                break;
            }

            // Honest validator should return certificates in the same order as the requested hashes.
            missing_hashes = missing_hashes[received.len()..].to_vec();
            certs_collected.append(&mut received);
        }
        Ok(certs_collected)
    }

    #[instrument(target = "grpc_client", skip_all, err, fields(address = self.address))]
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        let req = api::BlobId::try_from(blob_id)?;
        Ok(client_delegate!(self, blob_last_used_by, req)?.try_into()?)
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
