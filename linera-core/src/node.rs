// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

#[cfg(not(web))]
use futures::stream::BoxStream;
#[cfg(web)]
use futures::stream::LocalBoxStream as BoxStream;
use futures::stream::Stream;
use linera_base::{
    crypto::{CryptoError, CryptoHash, ValidatorPublicKey},
    data_types::{ArithmeticError, Blob, BlobContent, BlockHeight, NetworkDescription, Round},
    identifiers::{BlobId, ChainId, EventId},
};
use linera_chain::{
    data_types::BlockProposal,
    types::{
        ConfirmedBlock, ConfirmedBlockCertificate, GenericCertificate, LiteCertificate, Timeout,
        ValidatedBlock,
    },
    ChainError,
};
use linera_execution::{committee::Committee, ExecutionError};
use linera_version::VersionInfo;
use linera_views::ViewError;
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

    /// Processes a confirmed certificate.
    async fn handle_confirmed_certificate(
        &self,
        certificate: GenericCertificate<ConfirmedBlock>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a validated certificate.
    async fn handle_validated_certificate(
        &self,
        certificate: GenericCertificate<ValidatedBlock>,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a timeout certificate.
    async fn handle_timeout_certificate(
        &self,
        certificate: GenericCertificate<Timeout>,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Gets the version info for this validator node.
    async fn get_version_info(&self) -> Result<VersionInfo, NodeError>;

    /// Gets the network's description.
    async fn get_network_description(&self) -> Result<NetworkDescription, NodeError>;

    /// Subscribes to receiving notifications for a collection of chains.
    async fn subscribe(&self, chains: Vec<ChainId>) -> Result<Self::NotificationStream, NodeError>;

    // Uploads a blob. Returns an error if the validator has not seen a
    // certificate using this blob.
    async fn upload_blob(&self, content: BlobContent) -> Result<BlobId, NodeError>;

    /// Uploads the blobs to the validator.
    // Unfortunately, this doesn't compile as an async function: async functions in traits
    // don't play well with default implementations, apparently.
    // See also https://github.com/rust-lang/impl-trait-utils/issues/17
    fn upload_blobs(
        &self,
        blobs: Vec<Blob>,
    ) -> impl futures::Future<Output = Result<Vec<BlobId>, NodeError>> {
        let tasks: Vec<_> = blobs
            .into_iter()
            .map(|blob| self.upload_blob(blob.into()))
            .collect();
        futures::future::try_join_all(tasks)
    }

    /// Downloads a blob. Returns an error if the validator does not have the blob.
    async fn download_blob(&self, blob_id: BlobId) -> Result<BlobContent, NodeError>;

    /// Downloads a blob that belongs to a pending proposal or the locking block on a chain.
    async fn download_pending_blob(
        &self,
        chain_id: ChainId,
        blob_id: BlobId,
    ) -> Result<BlobContent, NodeError>;

    /// Handles a blob that belongs to a pending proposal or validated block certificate.
    async fn handle_pending_blob(
        &self,
        chain_id: ChainId,
        blob: BlobContent,
    ) -> Result<ChainInfoResponse, NodeError>;

    async fn download_certificate(
        &self,
        hash: CryptoHash,
    ) -> Result<ConfirmedBlockCertificate, NodeError>;

    /// Requests a batch of certificates from the validator.
    async fn download_certificates(
        &self,
        hashes: Vec<CryptoHash>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError>;

    /// Requests a batch of certificates from a specific chain by heights.
    async fn download_certificates_by_heights(
        &self,
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    ) -> Result<Vec<ConfirmedBlockCertificate>, NodeError>;

    /// Returns the hash of the `Certificate` that last used a blob.
    async fn blob_last_used_by(&self, blob_id: BlobId) -> Result<CryptoHash, NodeError>;

    /// Returns the certificate that last used the blob.
    async fn blob_last_used_by_certificate(
        &self,
        blob_id: BlobId,
    ) -> Result<ConfirmedBlockCertificate, NodeError>;

    /// Returns the missing `Blob`s by their IDs.
    async fn missing_blob_ids(&self, blob_ids: Vec<BlobId>) -> Result<Vec<BlobId>, NodeError>;
}

/// Turn an address into a validator node.
#[cfg_attr(not(web), trait_variant::make(Send + Sync))]
pub trait ValidatorNodeProvider: 'static {
    #[cfg(not(web))]
    type Node: ValidatorNode + Send + Sync + Clone + 'static;
    #[cfg(web)]
    type Node: ValidatorNode + Clone + 'static;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError>;

    fn make_nodes(
        &self,
        committee: &Committee,
    ) -> Result<impl Iterator<Item = (ValidatorPublicKey, Self::Node)> + '_, NodeError> {
        let validator_addresses: Vec<_> = committee
            .validator_addresses()
            .map(|(node, name)| (node, name.to_owned()))
            .collect();
        self.make_nodes_from_list(validator_addresses)
    }

    fn make_nodes_from_list<A>(
        &self,
        validators: impl IntoIterator<Item = (ValidatorPublicKey, A)>,
    ) -> Result<impl Iterator<Item = (ValidatorPublicKey, Self::Node)>, NodeError>
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
    #[error("The chain {0} is not active in validator")]
    InactiveChain(ChainId),

    #[error("Round number should be {0:?}")]
    WrongRound(Round),

    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },

    // This error must be normalized during conversions.
    #[error(
        "Cannot vote for block proposal of chain {chain_id} because a message \
         from chain {origin} at height {height} has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        origin: ChainId,
        height: BlockHeight,
    },

    #[error("Blobs not found: {0:?}")]
    BlobsNotFound(Vec<BlobId>),

    #[error("Events not found: {0:?}")]
    EventsNotFound(Vec<EventId>),

    // This error must be normalized during conversions.
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,

    #[error("Response doesn't contain requested certificates: {0:?}")]
    MissingCertificates(Vec<CryptoHash>),

    #[error("Validator's response failed to include a vote when trying to {0}")]
    MissingVoteInValidatorResponse(String),

    #[error("The received chain info response is invalid")]
    InvalidChainInfoResponse,
    #[error("Unexpected certificate value")]
    UnexpectedCertificateValue,

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

    #[error("Node failed to provide a 'last used by' certificate for the blob")]
    InvalidCertificateForBlob(BlobId),
    #[error("Node returned a BlobsNotFound error with duplicates")]
    DuplicatesInBlobsNotFound,
    #[error("Node returned a BlobsNotFound error with unexpected blob IDs")]
    UnexpectedEntriesInBlobsNotFound,
    #[error("Node returned certificates {returned:?}, but we requested {requested:?}")]
    UnexpectedCertificates {
        returned: Vec<CryptoHash>,
        requested: Vec<CryptoHash>,
    },
    #[error("Node returned a BlobsNotFound error with an empty list of missing blob IDs")]
    EmptyBlobsNotFound,
    #[error("Local error handling validator response: {error}")]
    ResponseHandlingError { error: String },

    #[error("Missing certificates for chain {chain_id} in heights {heights:?}")]
    MissingCertificatesByHeights {
        chain_id: ChainId,
        heights: Vec<BlockHeight>,
    },

    #[error("Too many certificates returned for chain {chain_id} from {remote_node}")]
    TooManyCertificatesReturned {
        chain_id: ChainId,
        remote_node: Box<ValidatorPublicKey>,
    },
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
        match self {
            CrossChainMessageDelivery::NonBlocking => false,
            CrossChainMessageDelivery::Blocking => true,
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
                origin,
                height,
            },
            ChainError::InactiveChain(chain_id) => Self::InactiveChain(chain_id),
            ChainError::ExecutionError(execution_error, context) => match *execution_error {
                ExecutionError::BlobsNotFound(blob_ids) => Self::BlobsNotFound(blob_ids),
                ExecutionError::EventsNotFound(event_ids) => Self::EventsNotFound(event_ids),
                _ => Self::ChainError {
                    error: ChainError::ExecutionError(execution_error, context).to_string(),
                },
            },
            ChainError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            } => Self::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            },
            ChainError::WrongRound(round) => Self::WrongRound(round),
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
            WorkerError::BlobsNotFound(blob_ids) => Self::BlobsNotFound(blob_ids),
            WorkerError::EventsNotFound(event_ids) => Self::EventsNotFound(event_ids),
            WorkerError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            } => NodeError::UnexpectedBlockHeight {
                expected_block_height,
                found_block_height,
            },
            error => Self::WorkerError {
                error: error.to_string(),
            },
        }
    }
}
