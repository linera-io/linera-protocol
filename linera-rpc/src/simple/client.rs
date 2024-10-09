// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::future::Future;

use async_trait::async_trait;
use futures::{sink::SinkExt, stream::StreamExt};
use linera_base::{
    crypto::CryptoHash,
    data_types::{Blob, BlobContent},
    identifiers::{BlobId, ChainId},
    time::{timer, Duration},
};
use linera_chain::data_types::{
    BlockProposal, Certificate, CertificateValue, HashedCertificateValue, LiteCertificate,
};
use linera_core::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    node::{CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode},
};
use linera_version::VersionInfo;

use super::{codec, transport::TransportProtocol};
use crate::{
    config::ValidatorPublicNetworkPreConfig, mass_client, HandleCertificateRequest,
    HandleLiteCertRequest, RpcMessage,
};

#[derive(Clone)]
pub struct SimpleClient {
    network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    send_timeout: Duration,
    recv_timeout: Duration,
}

impl SimpleClient {
    pub(crate) fn new(
        network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Self {
        Self {
            network,
            send_timeout,
            recv_timeout,
        }
    }

    async fn send_recv_internal(&self, message: RpcMessage) -> Result<RpcMessage, codec::Error> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = self.network.protocol.connect(address).await?;
        // Send message
        timer::timeout(self.send_timeout, stream.send(message))
            .await
            .map_err(|timeout| codec::Error::Io(timeout.into()))??;
        // Wait for reply
        timer::timeout(self.recv_timeout, stream.next())
            .await
            .map_err(|timeout| codec::Error::Io(timeout.into()))?
            .transpose()?
            .ok_or_else(|| codec::Error::Io(std::io::ErrorKind::UnexpectedEof.into()))
    }

    async fn query<Response>(&self, query: RpcMessage) -> Result<Response, Response::Error>
    where
        Response: TryFrom<RpcMessage>,
        Response::Error: From<codec::Error>,
    {
        self.send_recv_internal(query).await?.try_into()
    }
}

impl ValidatorNode for SimpleClient {
    type NotificationStream = NotificationStream;

    /// Initiates a new block.
    async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.query(proposal.into()).await
    }

    /// Processes a hash certificate.
    async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleLiteCertRequest {
            certificate: certificate.cloned(),
            wait_for_outgoing_messages,
        };
        self.query(request.into()).await
    }

    /// Processes a certificate.
    async fn handle_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError> {
        let wait_for_outgoing_messages = delivery.wait_for_outgoing_messages();
        let request = HandleCertificateRequest {
            certificate,
            blobs,
            wait_for_outgoing_messages,
        };
        self.query(request.into()).await
    }

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError> {
        self.query(query.into()).await
    }

    fn subscribe(
        &self,
        _chains: Vec<ChainId>,
    ) -> impl Future<Output = Result<NotificationStream, NodeError>> + Send {
        let transport = self.network.protocol.to_string();
        async { Err(NodeError::SubscriptionError { transport }) }
    }

    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        self.query(RpcMessage::VersionInfoQuery).await
    }

    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError> {
        self.query(RpcMessage::GenesisConfigHashQuery).await
    }

    async fn download_blob_content(&self, blob_id: BlobId) -> Result<BlobContent, NodeError> {
        self.query(RpcMessage::DownloadBlobContent(Box::new(blob_id)))
            .await
    }

    async fn download_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, NodeError> {
        let certificate_value: CertificateValue = self
            .query(RpcMessage::DownloadCertificateValue(Box::new(hash)))
            .await?;
        Ok(certificate_value.with_hash_checked(hash)?)
    }

    async fn download_certificate(&self, hash: CryptoHash) -> Result<Certificate, NodeError> {
        self.query(RpcMessage::DownloadCertificate(Box::new(hash)))
            .await
    }

    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        self.query(RpcMessage::BlobLastUsedBy(Box::new(blob_id)))
            .await
    }
}

#[derive(Clone)]
pub struct SimpleMassClient {
    pub network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
    send_timeout: Duration,
    recv_timeout: Duration,
}

impl SimpleMassClient {
    pub fn new(
        network: ValidatorPublicNetworkPreConfig<TransportProtocol>,
        send_timeout: Duration,
        recv_timeout: Duration,
    ) -> Self {
        Self {
            network,
            send_timeout,
            recv_timeout,
        }
    }
}

#[async_trait]
impl mass_client::MassClient for SimpleMassClient {
    async fn send(
        &self,
        requests: Vec<RpcMessage>,
        max_in_flight: usize,
    ) -> Result<Vec<RpcMessage>, mass_client::MassClientError> {
        let address = format!("{}:{}", self.network.host, self.network.port);
        let mut stream = self.network.protocol.connect(address).await?;
        let mut requests = requests.into_iter();
        let mut in_flight = 0;
        let mut responses = Vec::new();

        loop {
            while in_flight < max_in_flight {
                let request = match requests.next() {
                    None => {
                        if in_flight == 0 {
                            return Ok(responses);
                        }
                        // No more entries to send.
                        break;
                    }
                    Some(request) => request,
                };
                let status = timer::timeout(self.send_timeout, stream.send(request)).await;
                if let Err(error) = status {
                    tracing::error!("Failed to send request: {}", error);
                    continue;
                }
                in_flight += 1;
            }
            if requests.len() % 5000 == 0 && requests.len() > 0 {
                tracing::info!("In flight {} Remaining {}", in_flight, requests.len());
            }
            match timer::timeout(self.recv_timeout, stream.next()).await {
                Ok(Some(Ok(message))) => {
                    in_flight -= 1;
                    responses.push(message);
                }
                Ok(Some(Err(error))) => {
                    tracing::error!("Received error response: {}", error);
                }
                Ok(None) => {
                    tracing::info!("Socket closed by server");
                    return Ok(responses);
                }
                Err(error) => {
                    tracing::error!(
                        "Timeout while receiving response: {} (in flight: {})",
                        error,
                        in_flight
                    );
                }
            }
        }
    }
}
