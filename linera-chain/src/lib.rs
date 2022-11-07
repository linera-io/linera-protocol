// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod chain;
mod manager;
pub mod messages;

pub use chain::{ChainStateView, ChainStateViewContext, Event};
use linera_base::{
    crypto::CryptoError,
    messages::{ApplicationId, BlockHeight, ChainId, Origin, RoundNumber},
};
use linera_execution::ExecutionError;
use linera_views::views::ViewError;
pub use manager::{ChainManager, Outcome as ChainManagerOutcome};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ChainError {
    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),
    #[error("Base error: {0}")]
    BaseError(#[from] linera_base::error::Error),
    #[error("Error in view operation: {0}")]
    ViewError(#[from] ViewError),
    #[error("Execution error: {0}")]
    ExecutionError(#[from] ExecutionError),

    #[error("The chain being queried is not active {0:?}")]
    InactiveChain(ChainId),
    #[error(
        "Cannot vote for block proposal of chain {chain_id:?} because a message \
         from chain {origin:?} at height {height:?} (application {application_id:?}) \
         has not been received yet"
    )]
    MissingCrossChainUpdate {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
    },
    #[error(
        "The given incoming message from {origin:?} at height {height:?} and \
         index {index:?} (application {application_id:?}) is out of order"
    )]
    InvalidMessageOrder {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
        index: usize,
    },
    #[error(
        "Message in block proposal for {chain_id:?} does not match the order of received messages from \
        chain {origin:?}: was height {height:?} and index {index:?} \
        instead of {expected_height:?} and {expected_index:?} (application {application_id:?})"
    )]
    InvalidMessage {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
        index: usize,
        expected_height: BlockHeight,
        expected_index: usize,
    },
    #[error(
        "Message in block proposal for {chain_id:?} does not match received message from {origin:?} \
        at height {height:?} and index {index:?} (application {application_id:?})"
    )]
    InvalidMessageContent {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
        index: usize,
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
}
