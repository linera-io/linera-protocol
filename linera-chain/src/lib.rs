// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the state of a Linera chain, including cross-chain communication.

mod chain;
pub mod data_types;
mod inbox;
pub mod manager;
mod outbox;
#[cfg(with_testing)]
pub mod test;

pub use chain::ChainStateView;
use data_types::{Event, Origin};
use linera_base::{
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight, Round, Timestamp},
    identifiers::{ApplicationId, ChainId},
};
use linera_execution::ExecutionError;
use linera_views::views::ViewError;
use rand_distr::WeightedError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
    #[error("Arithmetic error: {0}")]
    ArithmeticError(#[from] ArithmeticError),
    #[error("Error in view operation: {0}")]
    ViewError(#[from] ViewError),
    #[error("Execution error: {0} during {1:?}")]
    ExecutionError(ExecutionError, ChainExecutionContext),

    #[error("The chain being queried is not active {0:?}")]
    InactiveChain(ChainId),
    #[error(
        "Cannot vote for block proposal of chain {chain_id:?} because a message \
         from origin {origin:?} at height {height:?} has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        origin: Box<Origin>,
        height: BlockHeight,
    },
    #[error(
        "Message in block proposed to {chain_id:?} does not match the previously received messages from \
        origin {origin:?}: was {event:?} instead of {previous_event:?}"
    )]
    UnexpectedMessage {
        chain_id: ChainId,
        origin: Box<Origin>,
        event: Event,
        previous_event: Event,
    },
    #[error(
        "Message in block proposed to {chain_id:?} is out of order compared to previous messages \
         from origin {origin:?}: {event:?}. Block and height should be at least: \
         {next_height}, {next_index}"
    )]
    IncorrectMessageOrder {
        chain_id: ChainId,
        origin: Box<Origin>,
        event: Event,
        next_height: BlockHeight,
        next_index: u32,
    },
    #[error("Block proposed to {chain_id:?} is attempting to reject protected message {event:?}")]
    CannotRejectMessage {
        chain_id: ChainId,
        origin: Box<Origin>,
        event: Event,
    },
    #[error(
        "Block proposed to {chain_id:?} is attempting to skip a message \
         that cannot be skipped: {event:?}"
    )]
    CannotSkipMessage {
        chain_id: ChainId,
        origin: Box<Origin>,
        event: Event,
    },
    #[error(
        "Incoming message in block proposed to {chain_id:?} has timestamp {message_timestamp:}, \
         which is later than the block timestamp {block_timestamp:}."
    )]
    IncorrectEventTimestamp {
        chain_id: ChainId,
        message_timestamp: Timestamp,
        block_timestamp: Timestamp,
    },
    #[error("The signature was not created by a valid entity")]
    InvalidSigner,
    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("The previous block hash of a new block should match the last block of the chain")]
    UnexpectedPreviousBlockHash,
    #[error("Sequence numbers above the maximal value are not usable for blocks")]
    InvalidBlockHeight,
    #[error("Block timestamp must not be earlier than the parent block's.")]
    InvalidBlockTimestamp,
    #[error("Cannot initiate a new block while the previous one is still pending confirmation")]
    PreviousBlockMustBeConfirmedFirst,
    #[error("Invalid block proposal")]
    InvalidBlockProposal,
    #[error("Round number should be at least {0:?}")]
    InsufficientRound(Round),
    #[error("Round number should greater than {0:?}")]
    InsufficientRoundStrict(Round),
    #[error("Round number should be {0:?}")]
    WrongRound(Round),
    #[error("A different block for height {0:?} was already locked at round number {1:?}")]
    HasLockedBlock(BlockHeight, Round),
    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },
    #[error("Signatures in a certificate must be from different validators")]
    CertificateValidatorReuse,
    #[error("Signatures in a certificate must form a quorum")]
    CertificateRequiresQuorum,
    #[error("Certificate signature verification failed: {error}")]
    CertificateSignatureVerificationFailed { error: String },
    #[error("Internal error {0}")]
    InternalError(String),
    #[error("Insufficient balance to pay the fees")]
    InsufficientBalance,
    #[error("Invalid owner weights: {0}")]
    OwnerWeightError(#[from] WeightedError),
    #[error("Closed chains cannot have operations, accepted messages or empty blocks")]
    ClosedChain,
    #[error("All operations on this chain must be from one of the following applications: {0:?}")]
    AuthorizedApplications(Vec<ApplicationId>),
    #[error("Can't use grant across different broadcast messages")]
    GrantUseOnBroadcast,
    #[error("ExecutedBlock contains fewer oracle responses than requests")]
    MissingOracleRecord,
}

#[derive(Copy, Clone, Debug)]
pub enum ChainExecutionContext {
    Query,
    DescribeApplication,
    IncomingMessage(u32),
    Operation(u32),
    Block,
    #[cfg(with_testing)]
    ReadBytecodeLocation,
}
