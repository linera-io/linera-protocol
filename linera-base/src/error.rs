// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    crypto::*,
    execution::Balance,
    messages::{BlockHeight, ChainId, Epoch, Origin, RoundNumber},
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
    #[error("The previous block hash of a new block should match the last block of the chain")]
    UnexpectedPreviousBlockHash,
    #[error("The height of a new block should increase the last block height of the chain by one")]
    UnexpectedBlockHeight,
    #[error("Sequence numbers above the maximal value are not usable for blocks.")]
    InvalidBlockHeight,
    #[error("Cannot initiate a new block while the previous one is still pending confirmation")]
    PreviousBlockMustBeConfirmedFirst,
    #[error("Cannot confirm a block before its predecessors: {current_block_height:?}")]
    MissingEarlierBlocks { current_block_height: BlockHeight },

    // Algorithmic operations
    #[error("Sequence number overflow.")]
    SequenceOverflow,
    #[error("Sequence number underflow.")]
    SequenceUnderflow,
    #[error("Amount overflow.")]
    AmountOverflow,
    #[error("Amount underflow.")]
    AmountUnderflow,
    #[error("Chain balance overflow.")]
    BalanceOverflow,
    #[error("Chain balance underflow.")]
    BalanceUnderflow,
    #[error("Operation is not supported for this chain.")]
    UnsupportedOperation,

    // Signatures and certificates
    #[error("Signature for object {type_name} is not valid: {error}")]
    InvalidSignature { error: String, type_name: String },
    #[error("The signature was not created by a valid entity")]
    InvalidSigner,
    #[error("Signatures in a certificate must form a quorum")]
    CertificateRequiresQuorum,
    #[error("Signatures in a certificate must be from different validators.")]
    CertificateValidatorReuse,
    #[error("The given certificate is invalid.")]
    InvalidCertificate,
    #[error("The given chain info response is invalid.")]
    InvalidChainInfoResponse,
    #[error("{epoch:?} is not recognized by chain {chain_id:}.")]
    InvalidEpoch { chain_id: ChainId, epoch: Epoch },

    // Validation of operations and blocks
    #[error("Transfers must have positive amount")]
    IncorrectTransferAmount,
    #[error(
        "The transferred amount must be not exceed the current chain balance: {current_balance:?}"
    )]
    InsufficientFunding { current_balance: Balance },
    #[error("Invalid new chain id: {0}")]
    InvalidNewChainId(ChainId),
    #[error("Invalid admin id in new chain: {0}")]
    InvalidNewChainAdminId(ChainId),
    #[error("Invalid subscription to new committees: {0}")]
    InvalidSubscriptionToNewCommittees(ChainId),
    #[error("Invalid unsubscription to new committees: {0}")]
    InvalidUnsubscriptionToNewCommittees(ChainId),
    #[error("Invalid committees")]
    InvalidCommittees,
    #[error("Failed to create new committee")]
    InvalidCommitteeCreation,
    #[error("Failed to remove committee")]
    InvalidCommitteeRemoval,
    #[error("Round number should be greater than {0:?}")]
    InsufficientRound(RoundNumber),
    #[error("A different block for height {0:?} was already locked at round number {1:?}")]
    HasLockedBlock(BlockHeight, RoundNumber),
    #[error(
        "This replica has not processed any update from {origin:?} \
        at height {height:?} yet"
    )]
    MissingCrossChainUpdate { origin: Origin, height: BlockHeight },
    #[error(
        "Message in block proposal does not match received message from {origin:?} \
        at height {height:?} and index {index:?}"
    )]
    InvalidMessageContent {
        origin: Origin,
        height: BlockHeight,
        index: usize,
    },
    #[error(
        "Message in block proposal does not match the order of received messages from \
        chain {origin:?}: was height {height:?} and index {index:?} \
        instead of {expected_height:?} and {expected_index:?})"
    )]
    InvalidMessage {
        origin: Origin,
        height: BlockHeight,
        index: usize,
        expected_height: BlockHeight,
        expected_index: usize,
    },
    #[error(
        "The given incoming message from {origin:?} at height {height:?} and \
         index {index:?} is out of order"
    )]
    InvalidMessageOrder {
        origin: Origin,
        height: BlockHeight,
        index: usize,
    },

    // Other server-side errors
    #[error("No certificate for this chain and block height")]
    CertificateNotFound,
    #[error("Invalid cross-chain request.")]
    InvalidCrossChainRequest,
    #[error("Invalid block proposal.")]
    InvalidBlockProposal,
    #[error("The block does contain the hash that we expected for the previous block.")]
    InvalidBlockChaining,
    #[error("The given state hash is not what we computed after executing the block")]
    IncorrectStateHash,
    #[error("The given effects are not what we computed after executing the block")]
    IncorrectEffects,

    // Client errors
    #[error("Client failed to obtain a valid response to the block proposal")]
    ClientErrorWhileProcessingBlockProposal,
    #[error("Client failed to obtain a valid response to the certificate request")]
    ClientErrorWhileQueryingCertificate,

    // Networking and sharding
    #[error("Wrong shard used.")]
    WrongShard,
    #[error("Cannot deserialize.")]
    InvalidDecoding,
    #[error("Unexpected message.")]
    UnexpectedMessage,
    #[error("Network error while querying service: {error:?}.")]
    ClientIoError { error: String },
    #[error("Storage error while querying service: {error:?}.")]
    StorageIoError { error: String },
    #[error("Storage (de)serialization error: {error:?}.")]
    StorageBcsError { error: String },

    // Storage
    #[error("Missing certificate: {hash:?}")]
    MissingCertificate { hash: HashValue },
}
