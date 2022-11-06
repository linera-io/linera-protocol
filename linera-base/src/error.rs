// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    crypto::CryptoError,
    messages::{ApplicationId, BlockHeight, ChainId, Epoch, Origin, RoundNumber},
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize, Error, Hash)]
/// Custom error type.
pub enum Error {
    // Chain access control
    #[error("The chain being queried is not active {0:?}")]
    InactiveChain(ChainId),
    #[error("Block was not signed by an authorized owner")]
    InvalidOwner,

    // Chaining
    #[error(
        "Was expecting block height {expected_block_height} but found {found_block_height} instead"
    )]
    UnexpectedBlockHeight {
        expected_block_height: BlockHeight,
        found_block_height: BlockHeight,
    },
    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },

    // Algorithmic operations
    #[error("Sequence number overflow")]
    SequenceOverflow,
    #[error("Sequence number underflow")]
    SequenceUnderflow,

    // Signatures and certificates
    #[error("Signature for object {type_name} is not valid: {error}")]
    InvalidSignature { error: String, type_name: String },
    #[error("The given certificate is invalid")]
    InvalidCertificate,
    #[error("The given chain info response is invalid")]
    InvalidChainInfoResponse,
    #[error("{epoch:?} is not recognized by chain {chain_id:}")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },

    // Validation of operations and blocks
    #[error("Round number should be greater than {0:?}")]
    InsufficientRound(RoundNumber),
    #[error("A different block for height {0:?} was already locked at round number {1:?}")]
    HasLockedBlock(BlockHeight, RoundNumber),
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
        "Message in block proposal does not match received message from {origin:?} \
        at height {height:?} and index {index:?} (application {application_id:?})"
    )]
    InvalidMessageContent {
        chain_id: ChainId,
        application_id: ApplicationId,
        origin: Origin,
        height: BlockHeight,
        index: usize,
    },

    // Other server-side errors
    #[error("Invalid cross-chain request")]
    InvalidCrossChainRequest,
    #[error("The block does contain the hash that we expected for the previous block")]
    InvalidBlockChaining,
    #[error("The given state hash is not what we computed after executing the block")]
    IncorrectStateHash,
    #[error("The given effects are not what we computed after executing the block")]
    IncorrectEffects,

    // Networking and sharding
    #[error("Cannot deserialize")]
    InvalidDecoding,
    #[error("Unexpected message")]
    UnexpectedMessage,
    #[error("Network error while querying service: {error}")]
    ClientIoError { error: String },
    #[error("Failed to resolve validator address: {address}")]
    CannotResolveValidatorAddress { address: String },

    // TODO(#148): Remove this.
    #[error("Error in view operation: {error}")]
    ViewError { error: String },

    #[error("Cryptographic error: {0}")]
    CryptoError(#[from] CryptoError),

    #[error("Execution error: {error}")]
    ExecutionError { error: String },
}
