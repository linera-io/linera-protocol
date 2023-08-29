// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data_types::{ChainInfoQuery, ChainInfoResponse},
    worker::{Notification, WorkerError},
};
use async_trait::async_trait;
use futures::Stream;
use linera_base::{
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight},
    identifiers::ChainId,
};
use linera_chain::{
    data_types::{BlockProposal, Certificate, HashedValue, LiteCertificate, Origin},
    ChainError,
};
use linera_execution::BytecodeLocation;
use linera_views::views::ViewError;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;

/// A pinned [`Stream`] of Notifications.
pub type NotificationStream = Pin<Box<dyn Stream<Item = Notification> + Send>>;

/// How to communicate with a validator or a local node.
#[async_trait]
pub trait ValidatorNode {
    /// Proposes a new block.
    async fn handle_block_proposal(
        &mut self,
        proposal: BlockProposal,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate without a value.
    async fn handle_lite_certificate(
        &mut self,
        certificate: LiteCertificate<'_>,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Subscribes to receiving notifications for a collection of chains.
    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError>;
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

    #[error("Grpc error: {error}")]
    GrpcError { error: String },

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

    // This error must be normalized during conversions.
    #[error("The following values containing application bytecode are missing: {0:?}.")]
    ApplicationBytecodesNotFound(Vec<BytecodeLocation>),

    #[error(
        "Failed to download the requested certificate(s) for chain {chain_id:?} \
         in order to advance to the next height {target_next_block_height}"
    )]
    CannotDownloadCertificates {
        chain_id: ChainId,
        target_next_block_height: BlockHeight,
    },

    #[error("Validator's response to block proposal failed to include a vote")]
    MissingVoteInValidatorResponse,

    // This should go to ClientError.
    #[error(
        "Failed to update validator because our local node doesn't have an active chain {0:?}"
    )]
    InactiveLocalChain(ChainId),

    // This should go to ClientError.
    #[error("The block does contain the hash that we expected for the previous block")]
    InvalidLocalBlockChaining,

    // This should go to ClientError.
    #[error("The given chain info response is invalid")]
    InvalidChainInfoResponse,

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still inactive \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockToInactiveChain { chain_id: ChainId, retries: usize },

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still missing messages \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockWithLaggingMessages { chain_id: ChainId, retries: usize },

    #[error(
        "Failed to submit block proposal: chain {chain_id:?} was still missing application bytecodes \
         after validator synchronization and {retries} retries"
    )]
    ProposedBlockWithLaggingBytecode { chain_id: ChainId, retries: usize },

    // Networking and sharding.
    // TODO(#258): Those probably belong to linera-service.
    #[error("Cannot deserialize")]
    InvalidDecoding,
    #[error("Unexpected message")]
    UnexpectedMessage,
    #[error("Network error while querying service: {error}")]
    ClientIoError { error: String },
    #[error("Failed to resolve validator address: {address}")]
    CannotResolveValidatorAddress { address: String },
    #[error("Subscription error due to incorrect transport. Was expecting gRPC, instead found: {transport}")]
    SubscriptionError { transport: String },
    #[error("Failed to subscribe; tonic status: {status}")]
    SubscriptionFailed { status: String },
    #[error("We don't have the value for the certificate.")]
    MissingCertificateValue,
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
            WorkerError::ApplicationBytecodesNotFound(locations) => {
                NodeError::ApplicationBytecodesNotFound(locations)
            }
            error => Self::WorkerError {
                error: error.to_string(),
            },
        }
    }
}
