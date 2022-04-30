// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, messages::Operation};
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
    #[fail(display = "Request was not signed by an authorized owner")]
    InvalidOwner,

    // Chain sequencing
    #[fail(
        display = "The sequence number in a request should match the next expected sequence number of the chain"
    )]
    UnexpectedSequenceNumber,
    #[fail(display = "Sequence numbers above the maximal value are not usable for requests.")]
    InvalidSequenceNumber,
    #[fail(
        display = "Cannot initiate a new request while the previous one is still pending confirmation"
    )]
    PreviousRequestMustBeConfirmedFirst,
    #[fail(
        display = "Cannot confirm a request before its predecessors: {:?}",
        current_sequence_number
    )]
    MissingEarlierConfirmations {
        current_sequence_number: SequenceNumber,
    },

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

    // Validation of operations and requests
    #[fail(display = "Transfers must have positive amount")]
    IncorrectTransferAmount,
    #[fail(
        display = "The transferred amount must be not exceed the current chain balance: {:?}",
        current_balance
    )]
    InsufficientFunding { current_balance: Balance },
    #[fail(display = "Invalid new chain id: {}", 0)]
    InvalidNewChainId(ChainId),
    #[fail(display = "Round number should be greater than {:?}", 0)]
    InsufficientRound(RoundNumber),
    #[fail(
        display = "A different operation was already locked for this chain and sequence number: {:?}",
        0
    )]
    HasLockedOperation(Operation),

    // Other server-side errors
    #[fail(display = "No certificate for this chain and sequence number")]
    CertificateNotFound,
    #[fail(display = "Invalid cross chain request.")]
    InvalidCrossChainRequest,
    #[fail(display = "Invalid block proposal.")]
    InvalidBlockProposal,

    // Client errors
    #[fail(display = "Client failed to obtain a valid response to the block proposal")]
    ClientErrorWhileProcessingBlockProposal,
    #[fail(display = "Client failed to obtain a valid response to the certificate request")]
    ClientErrorWhileRequestingCertificate,

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
