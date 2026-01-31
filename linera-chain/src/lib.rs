// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the state of a Linera chain, including cross-chain communication.

pub mod block;
mod certificate;

pub mod types {
    pub use super::{block::*, certificate::*};
}

mod block_tracker;
mod chain;
pub mod data_types;
mod inbox;
pub mod manager;
mod outbox;
mod pending_blobs;
#[cfg(with_testing)]
pub mod test;

pub use chain::ChainStateView;
use data_types::{MessageBundle, PostedMessage};
use linera_base::{
    bcs,
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight, Round, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_execution::{ExecutionError, ResourceTracker};
use linera_views::ViewError;
use thiserror::Error;

/// Context about a limit error, including resources used before the failed transaction
/// and the limit that was exceeded.
#[derive(Copy, Clone, Debug)]
pub struct LimitErrorContext {
    /// Resources consumed before the failed transaction.
    pub resources_before_error: ResourceTracker,
    /// The limit value that was exceeded.
    pub limit_exceeded: u64,
}

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
    #[error(transparent)]
    ArithmeticError(#[from] ArithmeticError),
    #[error(transparent)]
    ViewError(#[from] ViewError),
    /// An error during execution.
    ///
    /// The optional `LimitErrorContext` contains information about limit errors, including
    /// the resources used before the failed transaction and the limit that was exceeded.
    /// This is useful for determining whether the block was close to its resource limits.
    #[error("Execution error: {0} during {1:?}")]
    ExecutionError(
        Box<ExecutionError>,
        ChainExecutionContext,
        Option<Box<LimitErrorContext>>,
    ),

    #[error("The chain being queried is not active {0}")]
    InactiveChain(ChainId),
    #[error(
        "Cannot vote for block proposal of chain {chain_id} because a message \
         from chain {origin} at height {height} has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        origin: ChainId,
        height: BlockHeight,
    },
    #[error(
        "Message in block proposed to {chain_id} does not match the previously received messages from \
        origin {origin:?}: was {bundle:?} instead of {previous_bundle:?}"
    )]
    UnexpectedMessage {
        chain_id: ChainId,
        origin: ChainId,
        bundle: Box<MessageBundle>,
        previous_bundle: Box<MessageBundle>,
    },
    #[error(
        "Message in block proposed to {chain_id} is out of order compared to previous messages \
         from origin {origin:?}: {bundle:?}. Block and height should be at least: \
         {next_height}, {next_index}"
    )]
    IncorrectMessageOrder {
        chain_id: ChainId,
        origin: ChainId,
        bundle: Box<MessageBundle>,
        next_height: BlockHeight,
        next_index: u32,
    },
    #[error(
        "Block proposed to {chain_id} is attempting to reject protected message \
        {posted_message:?}"
    )]
    CannotRejectMessage {
        chain_id: ChainId,
        origin: ChainId,
        posted_message: Box<PostedMessage>,
    },
    #[error(
        "Block proposed to {chain_id} is attempting to skip a message bundle \
         that cannot be skipped: {bundle:?}"
    )]
    CannotSkipMessage {
        chain_id: ChainId,
        origin: ChainId,
        bundle: Box<MessageBundle>,
    },
    #[error(
        "Incoming message bundle in block proposed to {chain_id} has timestamp \
        {bundle_timestamp:}, which is later than the block timestamp {block_timestamp:}."
    )]
    IncorrectBundleTimestamp {
        chain_id: ChainId,
        bundle_timestamp: Timestamp,
        block_timestamp: Timestamp,
    },
    #[error("The signature was not created by a valid entity")]
    InvalidSigner,
    #[error(
        "Chain is expecting a next block at height {expected_block_height} but the given block \
        is at height {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("The previous block hash of a new block should match the last block of the chain")]
    UnexpectedPreviousBlockHash,
    #[error("Sequence numbers above the maximal value are not usable for blocks")]
    BlockHeightOverflow,
    #[error(
        "Block timestamp {new} must not be earlier than the parent block's timestamp {parent}"
    )]
    InvalidBlockTimestamp { parent: Timestamp, new: Timestamp },
    #[error("Round number should be at least {0:?}")]
    InsufficientRound(Round),
    #[error("Round number should be greater than {0:?}")]
    InsufficientRoundStrict(Round),
    #[error("Round number should be {0:?}")]
    WrongRound(Round),
    #[error("Already voted to confirm a different block for height {0:?} at round number {1:?}")]
    HasIncompatibleConfirmedVote(BlockHeight, Round),
    #[error("Proposal for height {0:?} is not newer than locking block in round {1:?}")]
    MustBeNewerThanLockingBlock(BlockHeight, Round),
    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },
    #[error("Signatures in a certificate must be from different validators")]
    CertificateValidatorReuse,
    #[error("Signatures in a certificate must form a quorum")]
    CertificateRequiresQuorum,
    #[error("Internal error {0}")]
    InternalError(String),
    #[error("Block proposal has size {0} which is too large")]
    BlockProposalTooLarge(usize),
    #[error(transparent)]
    BcsError(#[from] bcs::Error),
    #[error("Closed chains cannot have operations, accepted messages or empty blocks")]
    ClosedChain,
    #[error("Empty blocks are not allowed")]
    EmptyBlock,
    #[error("All operations on this chain must be from one of the following applications: {0:?}")]
    AuthorizedApplications(Vec<ApplicationId>),
    #[error("Missing operations or messages from mandatory applications: {0:?}")]
    MissingMandatoryApplications(Vec<ApplicationId>),
    #[error("Executed block contains fewer oracle responses than requests")]
    MissingOracleResponseList,
    #[error("Not signing timeout certificate; current round does not time out")]
    RoundDoesNotTimeOut,
    #[error("Not signing timeout certificate; current round times out at time {0}")]
    NotTimedOutYet(Timestamp),
}

impl ChainError {
    /// Returns whether this error is caused by an issue in the local node.
    ///
    /// Returns `false` whenever the error could be caused by a bad message from a peer.
    pub fn is_local(&self) -> bool {
        match self {
            ChainError::CryptoError(_)
            | ChainError::ArithmeticError(_)
            | ChainError::ViewError(ViewError::NotFound(_))
            | ChainError::InactiveChain(_)
            | ChainError::IncorrectMessageOrder { .. }
            | ChainError::CannotRejectMessage { .. }
            | ChainError::CannotSkipMessage { .. }
            | ChainError::IncorrectBundleTimestamp { .. }
            | ChainError::InvalidSigner
            | ChainError::UnexpectedBlockHeight { .. }
            | ChainError::UnexpectedPreviousBlockHash
            | ChainError::BlockHeightOverflow
            | ChainError::InvalidBlockTimestamp { .. }
            | ChainError::InsufficientRound(_)
            | ChainError::InsufficientRoundStrict(_)
            | ChainError::WrongRound(_)
            | ChainError::HasIncompatibleConfirmedVote(..)
            | ChainError::MustBeNewerThanLockingBlock(..)
            | ChainError::MissingEarlierBlocks { .. }
            | ChainError::CertificateValidatorReuse
            | ChainError::CertificateRequiresQuorum
            | ChainError::BlockProposalTooLarge(_)
            | ChainError::ClosedChain
            | ChainError::EmptyBlock
            | ChainError::AuthorizedApplications(_)
            | ChainError::MissingMandatoryApplications(_)
            | ChainError::MissingOracleResponseList
            | ChainError::RoundDoesNotTimeOut
            | ChainError::NotTimedOutYet(_)
            | ChainError::MissingCrossChainUpdate { .. } => false,
            ChainError::ViewError(_)
            | ChainError::UnexpectedMessage { .. }
            | ChainError::InternalError(_)
            | ChainError::BcsError(_) => true,
            ChainError::ExecutionError(execution_error, _, _) => execution_error.is_local(),
        }
    }
}

#[derive(Copy, Clone, Debug)]
#[cfg_attr(with_testing, derive(Eq, PartialEq))]
pub enum ChainExecutionContext {
    Query,
    DescribeApplication,
    IncomingBundle(u32),
    Operation(u32),
    Block,
}

pub trait ExecutionResultExt<T> {
    fn with_execution_context(self, context: ChainExecutionContext) -> Result<T, ChainError>;
}

impl<T, E> ExecutionResultExt<T> for Result<T, E>
where
    E: Into<ExecutionError>,
{
    fn with_execution_context(self, context: ChainExecutionContext) -> Result<T, ChainError> {
        self.map_err(|error| ChainError::ExecutionError(Box::new(error.into()), context, None))
    }
}

impl ChainError {
    /// Sets the limit error context for an execution error.
    ///
    /// This is used to record the resources consumed before a transaction failed
    /// and the limit that was exceeded, which helps determine whether the block
    /// was close to its resource limits.
    pub fn with_limit_error_context(
        self,
        resources_before_error: ResourceTracker,
        limit_exceeded: u64,
    ) -> Self {
        match self {
            ChainError::ExecutionError(error, context, _) => ChainError::ExecutionError(
                error,
                context,
                Some(Box::new(LimitErrorContext {
                    resources_before_error,
                    limit_exceeded,
                })),
            ),
            other => other,
        }
    }

    /// Returns the limit error context, if available.
    pub fn limit_error_context(&self) -> Option<&LimitErrorContext> {
        match self {
            ChainError::ExecutionError(_, _, context) => context.as_deref(),
            _ => None,
        }
    }
}
