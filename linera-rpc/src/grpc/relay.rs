// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Reaching other validators through this validator's own proxy.
//!
//! Shards hold the validator's secret key, so the proxy is the only component that should open
//! outbound connections. A shard needing another validator — today, a chain worker exporting a
//! block — sends its request to the proxy's existing internal port naming the intended validator,
//! and the proxy performs it and returns the answer.
//!
//! [`RelayClient`] therefore implements [`ValidatorNode`] only for the operations block export
//! uses; the rest are refused rather than silently doing something else. Retries are absent by
//! design, since the export task already backs off per destination.

use std::{
    collections::BTreeMap,
    str::FromStr as _,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use linera_base::{
    crypto::CryptoHash,
    data_types::{BlobContent, BlockHeight, NetworkDescription},
    identifiers::{BlobId, ChainId, EventId, StreamId},
};
use linera_chain::{data_types, types};
use linera_core::node::{
    BlobStream, CrossChainMessageDelivery, NodeError, NotificationStream, ValidatorNode,
    ValidatorNodeProvider,
};
use linera_version::VersionInfo;
use tonic::Request;
use tracing::{debug, instrument, Level};

use super::{
    api::{self, validator_relay_client::ValidatorRelayClient},
    pool::GrpcConnectionPool,
    push_client, transport, GrpcError, GRPC_MAX_MESSAGE_SIZE,
};
use crate::{
    config::ValidatorPublicNetworkConfig, node_provider::NodeOptions,
    HandleConfirmedCertificateRequest, HandleLiteCertRequest,
};

/// Refuses an operation that the relay deliberately does not carry.
fn unsupported(operation: &str) -> NodeError {
    NodeError::GrpcError {
        error: format!(
            "{operation} is not available through the validator relay, which only carries the \
             requests needed to export blocks"
        ),
    }
}

/// A validator reached through this validator's proxy.
#[derive(Clone)]
pub struct RelayClient {
    /// The destination validator's address as the committee spells it, e.g. `grpc:host:port`.
    /// Kept in the committee's own form so the proxy resolves it the way any other consumer
    /// would.
    destination: String,
    /// The same validator as a URL, used only to name it in logs and metrics.
    address: String,
    client: ValidatorRelayClient<transport::Channel>,
}

impl RelayClient {
    /// Turns the proxy's answer into the chain info the caller expects.
    fn try_into_chain_info(
        result: api::ChainInfoResult,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let inner = result.inner.ok_or_else(|| NodeError::GrpcError {
            error: "missing body from response".to_string(),
        })?;
        match inner {
            api::chain_info_result::Inner::ChainInfoResponse(response) => {
                Ok(response.try_into().map_err(|error| NodeError::GrpcError {
                    error: format!("failed to unmarshal response: {error}"),
                })?)
            }
            // bincode, matching what the validator used. Load-bearing: the recovery paths
            // dispatch on the *variant* — `EventsNotFound` pushes the admin chain,
            // `BlobsNotFound` uploads blobs — so a mangled error silently disables them.
            api::chain_info_result::Inner::Error(error) => Err(bincode::deserialize(&error)
                .map_err(|error| NodeError::GrpcError {
                    error: format!("failed to unmarshal error message: {error}"),
                })?),
        }
    }
}

impl ValidatorNode for RelayClient {
    type NotificationStream = NotificationStream;

    fn address(&self) -> String {
        self.address.clone()
    }

    type PushStream = push_client::PushStream;

    /// Opens a push stream through this validator's own proxy.
    ///
    /// The destination is named in the first message and only there, so the proxy checks it
    /// against the committee once per stream rather than once per certificate.
    async fn open_push_stream(&self) -> Result<Self::PushStream, NodeError> {
        let (certificates, queue) = push_client::write_half();
        let (relayed, relayed_queue) = tokio::sync::mpsc::channel(1);
        relayed
            .send(api::RelayPushRequest {
                inner: Some(api::relay_push_request::Inner::Destination(
                    self.destination.clone(),
                )),
            })
            .await
            .map_err(|_| NodeError::PushStreamClosed)?;
        // Wraps each certificate for the relay without the caller knowing there is one.
        tokio::spawn(async move {
            let mut queue = queue;
            while let Some(certificate) = queue.recv().await {
                let wrapped = api::RelayPushRequest {
                    inner: Some(api::relay_push_request::Inner::Certificate(certificate)),
                };
                if relayed.send(wrapped).await.is_err() {
                    return;
                }
            }
        });
        let responses = self
            .client
            .clone()
            .relay_confirmed_certificates(tokio_stream::wrappers::ReceiverStream::new(
                relayed_queue,
            ))
            .await
            .map_err(push_client::open_error)?
            .into_inner();
        Ok(push_client::PushStream::new(certificates, responses))
    }

    #[instrument(target = "relay_client", skip_all, err(level = Level::DEBUG), fields(destination = self.address))]
    async fn handle_lite_certificate(
        &self,
        certificate: types::LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let inner = HandleLiteCertRequest {
            certificate,
            wait_for_outgoing_messages: delivery.wait_for_outgoing_messages(),
        };
        let request = api::RelayLiteCertificateRequest {
            destination: self.destination.clone(),
            inner: Some(inner.try_into()?),
        };
        debug!(handler = "relay_lite_certificate", "sending gRPC request");
        let result = self
            .client
            .clone()
            .relay_lite_certificate(Request::new(request))
            .await?
            .into_inner();
        Self::try_into_chain_info(result)
    }

    #[instrument(target = "relay_client", skip_all, err(level = Level::DEBUG), fields(destination = self.address))]
    async fn handle_confirmed_certificate(
        &self,
        certificate: linera_storage::Arc<types::GenericCertificate<types::ConfirmedBlock>>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let inner = HandleConfirmedCertificateRequest {
            certificate: linera_storage::Arc::unwrap_or_clone(certificate),
            wait_for_outgoing_messages: delivery.wait_for_outgoing_messages(),
        };
        let request = api::RelayConfirmedCertificateRequest {
            destination: self.destination.clone(),
            inner: Some(inner.try_into()?),
        };
        debug!(
            handler = "relay_confirmed_certificate",
            "sending gRPC request"
        );
        let result = self
            .client
            .clone()
            .relay_confirmed_certificate(Request::new(request))
            .await?
            .into_inner();
        Self::try_into_chain_info(result)
    }

    #[instrument(target = "relay_client", skip_all, err(level = Level::DEBUG), fields(destination = self.address))]
    async fn handle_chain_info_query(
        &self,
        query: linera_core::data_types::ChainInfoQuery,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        let request = api::RelayChainInfoQueryRequest {
            destination: self.destination.clone(),
            inner: Some(query.try_into()?),
        };
        debug!(handler = "relay_chain_info_query", "sending gRPC request");
        let result = self
            .client
            .clone()
            .relay_chain_info_query(Request::new(request))
            .await?
            .into_inner();
        Self::try_into_chain_info(result)
    }

    #[instrument(target = "relay_client", skip(self), err(level = Level::DEBUG), fields(destination = self.address))]
    async fn upload_blob(&self, content: BlobContent) -> Result<BlobId, NodeError> {
        let request = api::RelayUploadBlobRequest {
            destination: self.destination.clone(),
            inner: Some(content.try_into()?),
        };
        debug!(handler = "relay_upload_blob", "sending gRPC request");
        let blob_id = self
            .client
            .clone()
            .relay_upload_blob(Request::new(request))
            .await?
            .into_inner();
        Ok(blob_id.try_into()?)
    }

    async fn handle_block_proposal(
        &self,
        _proposal: data_types::BlockProposal,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        Err(unsupported("handle_block_proposal"))
    }

    async fn handle_validated_certificate(
        &self,
        _certificate: types::GenericCertificate<types::ValidatedBlock>,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        Err(unsupported("handle_validated_certificate"))
    }

    async fn handle_timeout_certificate(
        &self,
        _certificate: types::GenericCertificate<types::Timeout>,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        Err(unsupported("handle_timeout_certificate"))
    }

    async fn get_version_info(&self) -> Result<VersionInfo, NodeError> {
        Err(unsupported("get_version_info"))
    }

    async fn get_network_description(&self) -> Result<NetworkDescription, NodeError> {
        Err(unsupported("get_network_description"))
    }

    async fn subscribe(
        &self,
        _chains: Vec<ChainId>,
    ) -> Result<Self::NotificationStream, NodeError> {
        Err(unsupported("subscribe"))
    }

    async fn download_blob(&self, _blob_id: BlobId) -> Result<BlobContent, NodeError> {
        Err(unsupported("download_blob"))
    }

    async fn download_blobs(&self, _blob_ids: Vec<BlobId>) -> Result<BlobStream, NodeError> {
        Err(unsupported("download_blobs"))
    }

    async fn download_pending_blob(
        &self,
        _chain_id: ChainId,
        _blob_id: BlobId,
    ) -> Result<BlobContent, NodeError> {
        Err(unsupported("download_pending_blob"))
    }

    async fn handle_pending_blob(
        &self,
        _chain_id: ChainId,
        _blob: BlobContent,
    ) -> Result<linera_core::data_types::ChainInfoResponse, NodeError> {
        Err(unsupported("handle_pending_blob"))
    }

    async fn download_certificate(
        &self,
        _hash: CryptoHash,
    ) -> Result<types::ConfirmedBlockCertificate, NodeError> {
        Err(unsupported("download_certificate"))
    }

    async fn download_certificates(
        &self,
        _hashes: Vec<CryptoHash>,
    ) -> Result<Vec<types::ConfirmedBlockCertificate>, NodeError> {
        Err(unsupported("download_certificates"))
    }

    async fn blob_last_used_by(&self, _blob_id: BlobId) -> Result<CryptoHash, NodeError> {
        Err(unsupported("blob_last_used_by"))
    }

    async fn missing_blob_ids(&self, _blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError> {
        Err(unsupported("missing_blob_ids"))
    }

    async fn blob_last_used_by_certificate(
        &self,
        _blob_id: BlobId,
    ) -> Result<types::ConfirmedBlockCertificate, NodeError> {
        Err(unsupported("blob_last_used_by_certificate"))
    }

    async fn download_certificates_by_heights(
        &self,
        _chain_id: ChainId,
        _heights: Vec<BlockHeight>,
    ) -> Result<Vec<types::ConfirmedBlockCertificate>, NodeError> {
        Err(unsupported("download_certificates_by_heights"))
    }

    async fn previous_event_blocks(
        &self,
        _chain_id: ChainId,
        _stream_ids: Vec<StreamId>,
    ) -> Result<BTreeMap<StreamId, (BlockHeight, CryptoHash)>, NodeError> {
        Err(unsupported("previous_event_blocks"))
    }

    async fn event_block_heights(
        &self,
        _event_ids: Vec<EventId>,
    ) -> Result<Vec<Option<BlockHeight>>, NodeError> {
        Err(unsupported("event_block_heights"))
    }
}

/// A node provider that reaches every validator through this validator's own proxies.
#[derive(Clone)]
pub struct RelayNodeProvider {
    /// The internal addresses of this validator's proxies — the same ones the shards already
    /// send notifications to.
    relay_addresses: Vec<String>,
    /// Round-robin cursor over `relay_addresses`. Each request goes through exactly one proxy,
    /// but successive nodes draw different ones so egress is spread. Shared across clones, so
    /// every chain worker in the process uses one rotation.
    next: Arc<AtomicUsize>,
    pool: GrpcConnectionPool,
}

impl RelayNodeProvider {
    /// Creates a provider that relays through the given proxies, in rotation. Panics if
    /// `relay_addresses` is empty: a shard must not open the connection itself.
    pub fn new(relay_addresses: Vec<String>, options: NodeOptions) -> Self {
        assert!(
            !relay_addresses.is_empty(),
            "relaying to other validators needs at least one proxy",
        );
        Self {
            relay_addresses,
            next: Arc::new(AtomicUsize::new(0)),
            pool: GrpcConnectionPool::new(transport::Options::from(&options)),
        }
    }

    /// Returns the next proxy in the rotation.
    fn next_relay(&self) -> &str {
        let index = self.next.fetch_add(1, Ordering::Relaxed) % self.relay_addresses.len();
        &self.relay_addresses[index]
    }
}

impl ValidatorNodeProvider for RelayNodeProvider {
    type Node = RelayClient;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError> {
        // Parsed even though the proxy is the one that dials it, so that a malformed committee
        // entry is rejected here rather than on every request.
        let network = ValidatorPublicNetworkConfig::from_str(address).map_err(|_| {
            NodeError::CannotResolveValidatorAddress {
                address: address.to_string(),
            }
        })?;
        let relay = self.next_relay();
        let channel = self
            .pool
            .channel(relay.to_owned())
            .map_err(|error: GrpcError| NodeError::GrpcError {
                error: format!("error creating channel to the relay {relay}: {error}"),
            })?;
        Ok(RelayClient {
            destination: address.to_owned(),
            address: network.http_address(),
            client: ValidatorRelayClient::new(channel)
                .max_encoding_message_size(GRPC_MAX_MESSAGE_SIZE)
                .max_decoding_message_size(GRPC_MAX_MESSAGE_SIZE),
        })
    }
}
