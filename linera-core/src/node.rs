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
use linera_execution::{
    committee::{Committee, ValidatorName},
    BytecodeLocation,
};
use linera_views::views::ViewError;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use thiserror::Error;

/// A pinned [`Stream`] of Notifications.
pub type NotificationStream = Pin<Box<dyn Stream<Item = Notification> + Send>>;

/// Whether to wait for the delivery of outgoing cross-chain messages.
#[derive(Debug, Default, Clone, Copy)]
pub enum CrossChainMessageDelivery {
    #[default]
    NonBlocking,
    Blocking,
}

/// How to communicate with a validator node.
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
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Processes a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
        blobs: Vec<HashedValue>,
        delivery: CrossChainMessageDelivery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Handles information queries for this chain.
    async fn handle_chain_info_query(
        &mut self,
        query: ChainInfoQuery,
    ) -> Result<ChainInfoResponse, NodeError>;

    /// Subscribes to receiving notifications for a collection of chains.
    async fn subscribe(&mut self, chains: Vec<ChainId>) -> Result<NotificationStream, NodeError>;
}

/// Turn an address into a validator node.
#[allow(clippy::result_large_err)]
pub trait ValidatorNodeProvider {
    type Node: ValidatorNode + Clone + Send + Sync + 'static;

    fn make_node(&self, address: &str) -> Result<Self::Node, NodeError>;

    fn make_nodes<I>(&self, committee: &Committee) -> Result<I, NodeError>
    where
        I: FromIterator<(ValidatorName, Self::Node)>,
    {
        committee
            .validators()
            .iter()
            .map(|(name, validator)| {
                let node = self.make_node(&validator.network_address)?;
                Ok((*name, node))
            })
            .collect()
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

    // This error must be normalized during conversions.
    #[error("The following values containing application bytecode are missing: {0:?}.")]
    ApplicationBytecodesNotFound(Vec<BytecodeLocation>),

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
