// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod chain;
pub mod data_types;
mod inbox;
mod manager;
mod outbox;

pub use chain::ChainStateView;
use data_types::{Event, Origin};
use linera_base::{
    crypto::CryptoError,
    data_types::{ArithmeticError, BlockHeight, ChainId, RoundNumber},
};
use linera_execution::{ApplicationId, ExecutionError};
use linera_views::views::ViewError;
pub use manager::{ChainManager, Outcome as ChainManagerOutcome};
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

    #[error("The chain being queried is not active {0:?}")]
    InactiveChain(ChainId),
    #[error(
        "Cannot vote for block proposal of chain {chain_id:?} because a message \
         from application {application_id:?} and origin {origin:?} at height {height:?} has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
    },
    #[error(
        "Message in block proposed to {chain_id:?} does not match the previously received messages from \
        application {application_id:?} and origin {origin:?}: was {event:?} instead of {previous_event:?}"
    )]
    UnexpectedMessage {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        event: Event,
        previous_event: Event,
    },
    #[error(
        "Message in block proposed to {chain_id:?} is out of order compared to previous messages from \
         application {application_id:?} and origin {origin:?}: {event:?}. Block and height should be at least: {next_height}, {next_index}"
    )]
    IncorrectMessageOrder {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        event: Event,
        next_height: BlockHeight,
        next_index: usize,
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
}
