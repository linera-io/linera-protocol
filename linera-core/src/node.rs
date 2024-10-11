// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(not(web))]
use futures::stream::BoxStream;
#[cfg(web)]
use futures::stream::LocalBoxStream as BoxStream;
use futures::stream::Stream;
use linera_base::{
    crypto::{CryptoError, CryptoHash},
    data_types::{ArithmeticError, Blob, BlobContent, BlockHeight},
    identifiers::{BlobId, ChainId},
};
use linera_chain::{
    data_types::{BlockProposal, Certificate, HashedCertificateValue, LiteCertificate, Origin},
    ChainError,
};
use linera_execution::{
    committee::{Committee, ValidatorName},
    ExecutionError, SystemExecutionError,
};
use linera_version::VersionInfo;
use linera_views::views::ViewError;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    worker::{Notification, WorkerError},
};

/// A pinned [`Stream`] of Notifications.
pub type NotificationStream = BoxStream<'static, Notification>;

/// Whether to wait for the delivery of outgoing cross-chain messages.
#[derive(Debug, Default, Clone, Copy)]
pub enum CrossChainMessageDelivery {
    #[default]
    NonBlocking,
    Blocking,
}

/// How to communicate with a validator node.
#[allow(async_fn_in_trait)]
#[cfg_attr(not(web), trait_variant::make(Send))]
pub trait ValidatorNode {
    #[cfg(not(web))]
    type NotificationStream: Stream<Item = Notification> + Unpin + Send;
    #[cfg(web)]
    type NotificationStream: Stream<Item = Notification> + Unpin;

    /// Proposes a new block.
    async fn handle_block_proposal(
        &self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate without a value.
    async fn handle_lite_certificate(
        &self,
        certificate: LiteCertificate<'_>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate.
    async fn handle_certificate(
        &self,
        certificate: Certificate,
        blobs: Vec<Blob>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Gets the version info for this validator node.
    async fn get_version_info(&self) -> Result<VersionInfo, NodeError>;

    /// Gets the network's genesis config hash.
    async fn get_genesis_config_hash(&self) -> Result<CryptoHash, NodeError>;

    /// Subscribes to receiving notifications for a collection of chains.
    async fn subscribe(&self, chains: Vec<ChainId>) -> Result<Self::NotificationStream, NodeError>;

    async fn download_blob_content(&self, blob_id: BlobId) -> Result<BlobContent, NodeError>;

    async fn download_certificate_value(
        &self,
        hash: CryptoHash,
    ) -> Result<HashedCertificateValue, NodeError>;

    async fn download_certificate(&self, hash: CryptoHash) -> Result<Certificate, NodeError>;

    /// Returns the hash of the `Certificate` that last used a blob.
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError>;
}

/// Turn an address into a validator node.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
#[expect(clippy::result_large_err)]
pub trait ValidatorNodeProvider: 'static {
    #[cfg(not(web))]
    type Node: ValidatorNode + Send + Sync + Clone + 'static;
    #[cfg(web)]
    type Node: ValidatorNode + Clone + 'static;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError>;

    fn make_nodes(
        &self,
        committee: &Committee,
    ) -> Result<impl Iterator<Item = (ValidatorName, Self::Node)> + '_, NodeError> {
        let validator_addresses: Vec<_> = committee
            .validator_addresses()
            .map(|(node, name)| (node, name.to_owned()))
            .collect();
        self.make_nodes_from_list(validator_addresses)
    }

    fn make_nodes_from_list<A>(
        &self,
        validators: impl IntoIterator<Item = (ValidatorName, A)>,
    ) -> Result<impl Iterator<Item = (ValidatorName, Self::Node)>, NodeError>
    where
        A: AsRef<str>,
    {
        Ok(validators
            .into_iter()
            .map(|(name, address)| Ok((name, self.make_node(address.as_ref())?)))
            .collect::<Result<Vec<_>, NodeError>>()?
            .into_iter())
    }
}

/// Error type for node queries.
///
/// This error is meant to be serialized over the network and aggregated by clients (i.e.
/// clients will track validator votes on each error value).
#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize, Error, Hash)]
pub enum NodeError {
    #[error("Cryptographic error: {error}")]
    CryptoError { error: String },

    #[error("Arithmetic error: {error}")]
    ArithmeticError { error: String },

    #[error("Error while accessing storage: {error}")]
    ViewError { error: String },

    #[error("Chain error: {error}")]
    ChainError { error: String },

    #[error("Worker error: {error}")]
    WorkerError { error: String },

    // This error must be normalized during conversions.
    #[error("The chain {0:?} is not active in validator")]
    InactiveChain(ChainId),

    // This error must be normalized during conversions.
    #[error(
        "Cannot vote for block proposal of chain {chain_id:?} because a message \
         from chain {origin:?} at height {height:?} has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        origin: Origin,
        height: BlockHeight,
    },

    #[error("The following blobs are missing: {0:?}.")]
    BlobsNotFound(Vec<BlobId>),

    // This error must be normalized during conversions.
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,

    #[error("Validator's response to block proposal failed to include a vote")]
    MissingVoteInValidatorResponse,

    #[error(
        "Failed to update validator because our local node doesn't have an active chain {0:?}"
    )]
    InactiveLocalChain(ChainId),

    #[error("The received chain info response is invalid")]
    InvalidChainInfoResponse,

    // Networking errors.
    // TODO(#258): These errors should be defined in linera-rpc.
    #[error("Cannot deserialize")]
    InvalidDecoding,
    #[error("Unexpected message")]
    UnexpectedMessage,
    #[error("Grpc error: {error}")]
    GrpcError { error: String },
    #[error("Network error while querying service: {error}")]
    ClientIoError { error: String },
    #[error("Failed to resolve validator address: {address}")]
    CannotResolveValidatorAddress { address: String },
    #[error("Subscription error due to incorrect transport. Was expecting gRPC, instead found: {transport}")]
    SubscriptionError { transport: String },
    #[error("Failed to subscribe; tonic status: {status}")]
    SubscriptionFailed { status: String },

    #[error("Failed to make a chain info query on the local node: {error}")]
    LocalNodeQuery { error: String },

    #[error("Blob not found on storage read: {0}")]
    BlobNotFoundOnRead(BlobId),
    #[error("Node failed to provide a 'last used by' certificate for the blob")]
    InvalidCertificateForBlob(BlobId),
}

impl From<tonic::Status> for NodeError {
    fn from(status: tonic::Status) -> Self {
        Self::GrpcError {
            error: status.to_string(),
        }
    }
}

impl CrossChainMessageDelivery {
    pub fn new(wait_for_outgoing_messages: bool) -> Self {
        if wait_for_outgoing_messages {
            CrossChainMessageDelivery::Blocking
        } else {
            CrossChainMessageDelivery::NonBlocking
        }
    }

    pub fn wait_for_outgoing_messages(self) -> bool {
        use CrossChainMessageDelivery::*;
        match self {
            NonBlocking => false,
            Blocking => true,
        }
    }
}

impl From<ViewError> for NodeError {
    fn from(error: ViewError) -> Self {
        Self::ViewError {
            error: error.to_string(),
        }
    }
}

impl From<ArithmeticError> for NodeError {
    fn from(error: ArithmeticError) -> Self {
        Self::ArithmeticError {
            error: error.to_string(),
        }
    }
}

impl From<CryptoError> for NodeError {
    fn from(error: CryptoError) -> Self {
        Self::CryptoError {
            error: error.to_string(),
        }
    }
}

impl From<ChainError> for NodeError {
    fn from(error: ChainError) -> Self {
        match error {
            ChainError::MissingCrossChainUpdate {
                chain_id,
                origin,
                height,
            } => Self::MissingCrossChainUpdate {
                chain_id,
                origin: *origin,
                height,
            },
            ChainError::InactiveChain(chain_id) => Self::InactiveChain(chain_id),
            ChainError::ExecutionError(
                ExecutionError::SystemError(SystemExecutionError::BlobNotFoundOnRead(blob_id)),
                _,
            ) => Self::BlobNotFoundOnRead(blob_id),
            error => Self::ChainError {
                error: error.to_string(),
            },
        }
    }
}

impl From<WorkerError> for NodeError {
    fn from(error: WorkerError) -> Self {
        match error {
            WorkerError::ChainError(error) => (*error).into(),
            WorkerError::MissingCertificateValue => Self::MissingCertificateValue,
            WorkerError::BlobsNotFound(blob_ids) => NodeError::BlobsNotFound(blob_ids),
            error => Self::WorkerError {
                error: error.to_string(),
            },
        }
    }
}
