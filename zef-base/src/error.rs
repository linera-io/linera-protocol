// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::base_types::*;
use failure::Fail;
use serde::{Deserialize, Serialize};

#[macro_export]
macro_rules! ensure {
    ($cond:expr, $e:expr) => {
        if !($cond) {
            return Err($e);
        }
    };
}

#[derive(Eq, PartialEq, Clone, Debug, Serialize, Deserialize, Fail, Hash)]
/// Custom error type.
pub enum Error {
    // Chain access control
    #[fail(display = "The chain being queried is not active {:?}", 0)]
    InactiveChain(ChainId),
    #[fail(display = "Block was not signed by an authorized owner")]
    InvalidOwner,

    // Chaining
    #[fail(
        display = "The previous block hash of a new block should match the last block of the chain"
    )]
    UnexpectedPreviousBlockHash,
    #[fail(
        display = "The height of a new block should increase the last block height of the chain by one"
    )]
    UnexpectedBlockHeight,
    #[fail(display = "Sequence numbers above the maximal value are not usable for blocks.")]
    InvalidBlockHeight,
    #[fail(
        display = "Cannot initiate a new block while the previous one is still pending confirmation"
    )]
    PreviousBlockMustBeConfirmedFirst,
    #[fail(
        display = "Cannot confirm a block before its predecessors: {:?}",
        current_block_height
    )]
    MissingEarlierBlocks { current_block_height: BlockHeight },

    // Algorithmic operations
    #[fail(display = "Sequence number overflow.")]
    SequenceOverflow,
    #[fail(display = "Sequence number underflow.")]
    SequenceUnderflow,
    #[fail(display = "Amount overflow.")]
    AmountOverflow,
    #[fail(display = "Amount underflow.")]
    AmountUnderflow,
    #[fail(display = "Chain balance overflow.")]
    BalanceOverflow,
    #[fail(display = "Chain balance underflow.")]
    BalanceUnderflow,

    // Signatures and certificates
    #[fail(display = "Signature for object {} is not valid: {}", type_name, error)]
    InvalidSignature { error: String, type_name: String },
    #[fail(display = "The signature was not created by a valid entity")]
    InvalidSigner,
    #[fail(display = "Signatures in a certificate must form a quorum")]
    CertificateRequiresQuorum,
    #[fail(display = "Signatures in a certificate must be from different validators.")]
    CertificateValidatorReuse,
    #[fail(display = "The given certificate is invalid.")]
    InvalidCertificate,
    #[fail(display = "The given chain info response is invalid.")]
    InvalidChainInfoResponse,

    // Validation of operations and blocks
    #[fail(display = "Transfers must have positive amount")]
    IncorrectTransferAmount,
    #[fail(
        display = "The transferred amount must be not exceed the current chain balance: {:?}",
        current_balance
    )]
    InsufficientFunding { current_balance: Balance },
    #[fail(display = "Invalid new chain id: {}", 0)]
    InvalidNewChainId(ChainId),
    #[fail(display = "Invalid committee")]
    InvalidCommittee,
    #[fail(display = "Round number should be greater than {:?}", 0)]
    InsufficientRound(RoundNumber),
    #[fail(
        display = "A different block for height {:?} was already locked at round number {:?}",
        0, 1
    )]
    HasLockedBlock(BlockHeight, RoundNumber),
    #[fail(
        display = "This replica has not processed any update from chain {:?} at height {:?} yet",
        sender_id, height
    )]
    MissingCrossChainUpdate {
        sender_id: ChainId,
        height: BlockHeight,
    },
    #[fail(
        display = "Message in block proposal does not match received message from chain {:?} at height {:?} and index {:?}",
        sender_id, height, index
    )]
    InvalidMessageContent {
        sender_id: ChainId,
        height: BlockHeight,
        index: usize,
    },
    #[fail(
        display = "Message in block proposal does not match the order of received messages from chain {:?}: was height {:?} and index {:?} instead of {:?} and {:?})",
        sender_id, height, index, expected_height, expected_index
    )]
    InvalidMessageOrder {
        sender_id: ChainId,
        height: BlockHeight,
        index: usize,
        expected_height: BlockHeight,
        expected_index: usize,
    },

    // Other server-side errors
    #[fail(display = "No certificate for this chain and block height")]
    CertificateNotFound,
    #[fail(display = "Invalid cross-chain request.")]
    InvalidCrossChainRequest,
    #[fail(display = "Invalid block proposal.")]
    InvalidBlockProposal,
    #[fail(display = "Chaining between blocks appears to be inconsistent despite the certificate")]
    InvalidBlockChaining,

    // Client errors
    #[fail(display = "Client failed to obtain a valid response to the block proposal")]
    ClientErrorWhileProcessingBlockProposal,
    #[fail(display = "Client failed to obtain a valid response to the certificate request")]
    ClientErrorWhileQueryingCertificate,

    // Networking and sharding
    #[fail(display = "Wrong shard used.")]
    WrongShard,
    #[fail(display = "Cannot deserialize.")]
    InvalidDecoding,
    #[fail(display = "Unexpected message.")]
    UnexpectedMessage,
    #[fail(display = "Network error while querying service: {:?}.", error)]
    ClientIoError { error: String },
    #[fail(display = "Storage error while querying service: {:?}.", error)]
    StorageIoError { error: String },
    #[fail(display = "Storage (de)serialization error: {:?}.", error)]
    StorageBcsError { error: String },

    // Storage
    #[fail(display = "Missing certificate: {:?}", hash)]
    MissingCertificate { hash: HashValue },
}
