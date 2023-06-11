// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! This module manages the state of a Linera chain, including cross-chain communication.

mod chain;
pub mod data_types;
mod inbox;
mod manager;
mod outbox;

pub use chain::ChainStateView;
use data_types::{Event, Origin};
use linera_base::{
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight, RoundNumber, Timestamp},
    identifiers::ChainId,
};
use linera_execution::{pricing::PricingError, ExecutionError};
use linera_views::views::ViewError;
pub use manager::{ChainManager, ChainManagerInfo, Outcome as ChainManagerOutcome};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
    #[error("Arithmetic error: {0}")]
    ArithmeticError(#[from] ArithmeticError),
    #[error("Error in view operation: {0}")]
    ViewError(#[from] ViewError),
    #[error("Execution error: {0}")]
    ExecutionError(#[from] ExecutionError),
    #[error("Pricing error: {0}")]
    PricingError(#[from] PricingError),

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
    #[error(
        "Incoming message in block proposed to {chain_id:?} has timestamp {message_timestamp:},
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
    #[error("Round number should be greater than {0:?}")]
    InsufficientRound(RoundNumber),
    #[error("A different block for height {0:?} was already locked at round number {1:?}")]
    HasLockedBlock(BlockHeight, RoundNumber),
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
}
