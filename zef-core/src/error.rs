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
    // Account access control
    #[fail(display = "The account being queried is not active {:?}", 0)]
    InactiveAccount(AccountId),
    #[fail(display = "Request was not signed by an authorized owner")]
    InvalidOwner,

    // Account sequencing
    #[fail(
        display = "The sequence number in a request should match the next expected sequence number of the account"
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

    // Smart-contract sequencing
    #[fail(display = "Transaction index must increase by one")]
    UnexpectedTransactionIndex,

    // Algorithmic operations
    #[fail(display = "Sequence number overflow.")]
    SequenceOverflow,
    #[fail(display = "Sequence number underflow.")]
    SequenceUnderflow,
    #[fail(display = "Amount overflow.")]
    AmountOverflow,
    #[fail(display = "Amount underflow.")]
    AmountUnderflow,
    #[fail(display = "Account balance overflow.")]
    BalanceOverflow,
    #[fail(display = "Account balance underflow.")]
    BalanceUnderflow,

    // Signatures and certificates
    #[fail(display = "Signature for object {} is not valid: {}", type_name, error)]
    InvalidSignature { error: String, type_name: String },
    #[fail(display = "Value was not signed by a known authority")]
    UnknownSigner,
    #[fail(display = "Signatures in a certificate must form a quorum")]
    CertificateRequiresQuorum,
    #[fail(display = "Signatures in a certificate must be from different authorities.")]
    CertificateAuthorityReuse,

    // Validation of operations and requests
    #[fail(display = "Transfers must have positive amount")]
    IncorrectTransferAmount,
    #[fail(
        display = "The transferred amount must be not exceed the current account balance: {:?}",
        current_balance
    )]
    InsufficientFunding { current_balance: Balance },
    #[fail(display = "Invalid new account id: {}", 0)]
    InvalidNewAccountId(AccountId),

    // Other server-side errors
    #[fail(display = "No certificate for this account and sequence number")]
    CertificateNotFound,
    #[fail(display = "Invalid cross shard request.")]
    InvalidCrossShardRequest,
    #[fail(display = "Invalid request order.")]
    InvalidRequestOrder,
    #[fail(display = "Invalid confirmation order.")]
    InvalidConfirmationOrder,
    #[fail(display = "Invalid coin creation order.")]
    InvalidCoinCreationOrder,

    // Client errors
    #[fail(display = "Client failed to obtain a valid response to the request order")]
    ClientErrorWhileProcessingRequestOrder,
    #[fail(display = "Client failed to obtain a valid response to the coin creation order")]
    ClientErrorWhileProcessingCoinCreationOrder,
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

    // Consensus
    #[fail(display = "Unknown consensus instance {:?}", 0)]
    UnknownConsensusInstance(AccountId),
    #[fail(display = "Invalid consensus order.")]
    InvalidConsensusOrder,
    #[fail(display = "Unsafe consensus proposal.")]
    UnsafeConsensusProposal,
    #[fail(
        display = "The following account has not been locked yet: {}",
        account_id
    )]
    MissingConsensusLock { account_id: AccountId },
    #[fail(display = "Invalid consensus proposal.")]
    InvalidConsensusProposal,
    #[fail(display = "Unsafe consensus pre-commit.")]
    UnsafeConsensusPreCommit,

    // Storage
    #[fail(display = "Missing certificate: {:?}", hash)]
    MissingCertificate { hash: HashValue },

    #[fail(display = "Missing consensus instance: {}", id)]
    MissingConsensusInstance { id: InstanceId },
}
