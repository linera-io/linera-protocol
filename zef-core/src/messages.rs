// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{base_types::*, committee::Committee, ensure, error::Error, account::AccountManager};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

#[cfg(test)]
#[path = "unit_tests/messages_tests.rs"]
mod messages_tests;

/// A recipient's address.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Address {
    Burn, // for demo purposes
    Account(AccountId),
}

/// An account operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Transfer `amount` units of value to the recipient.
    Transfer {
        recipient: Address,
        amount: Amount,
        user_data: UserData,
    },
    /// Create (or activate) a new account by installing the given authentication key.
    OpenAccount {
        new_id: AccountId,
        new_owner: AccountOwner,
    },
    /// Do nothing. (This can be used for testing or to unlock accounts after a consensus decisions.)
    Skip,
    /// Close the account.
    CloseAccount,
    /// Change the authentication key of the account.
    ChangeOwner { new_owner: AccountOwner },
    /// Start a consensus protocol `new_id` to manage the given `accounts`.
    StartConsensusInstance {
        new_id: AccountId,
        functionality: Functionality,
    },
    /// Lock the account into the given consensus instance, managed by the given key.
    LockInto {
        instance_id: AccountId,
        owner: AccountOwner,
    },
}

/// A one-shot funtionality implemented in a consensus instance.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Functionality {
    AtomicSwap {
        accounts: Vec<(AccountId, SequenceNumber)>,
    },
}

/// A request containing an account operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Request {
    pub account_id: AccountId,
    pub operation: Operation,
    pub sequence_number: SequenceNumber,
}

/// The content of a request to be signed in a RequestOrder.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct RequestValue {
    pub request: Request,
    pub limited_to: Option<AuthorityName>,
}

/// An authenticated request plus additional certified assets.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct RequestOrder {
    pub value: RequestValue,
    pub owner: AccountOwner,
    pub signature: Signature,
}

/// Functionality-dependent consensus decision.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, Serialize, Deserialize)]
pub enum ConsensusDecision {
    Abort,
    Confirm,
}

/// The proposal of a particular decision during consensus.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct ConsensusProposal {
    pub instance_id: AccountId,
    pub round: SequenceNumber,
    pub decision: ConsensusDecision,
}

/// A statement to be certified by the authorities.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Value {
    // -- Account management --
    /// The request was validated but confirmation will require additional steps.
    Lock(Request),
    /// The request is ready to be confirmed (i.e. executed).
    Confirm(Request),
    // -- Consensus --
    /// The proposal was validated but confirmation will require additional steps.
    PreCommit {
        proposal: ConsensusProposal,
        requests: Vec<Request>,
    },
    /// The proposal is ready to be committed, thus confirming a number of requests.
    Commit {
        proposal: ConsensusProposal,
        requests: Vec<Request>,
    },
}

/// Same as RequestOrder but meant for a consensus instance.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum ConsensusOrder {
    Propose {
        proposal: ConsensusProposal,
        owner: AccountOwner,
        signature: Signature,
        locks: Vec<Certificate>,
    },
    HandlePreCommit {
        certificate: Certificate,
    },
    HandleCommit {
        certificate: Certificate,
        locks: Vec<Certificate>,
    },
    GetStatus {
        instance_id: AccountId,
    },
}

/// The response to a consensus order.
#[allow(clippy::large_enum_variant)]
pub enum ConsensusResponse {
    Info(ConsensusInfoResponse),
    Vote(Vote),
    Continuation(Vec<CrossShardRequest>),
}

/// Current status of a consensus instance.
/// TODO: Information on available rounds.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConsensusInfoResponse {
    pub locked_accounts: BTreeMap<AccountId, AccountOwner>,
    pub proposed: Option<ConsensusProposal>,
    pub locked: Option<Certificate>,
    pub received: Certificate,
}

/// A vote on a statement from an authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct Vote {
    pub value: Value,
    pub authority: AuthorityName,
    pub signature: Signature,
}

/// A certified statement from the committee. Note: Opaque coins have no external
/// signatures and are authenticated at a lower level.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: Value,
    /// Hash of the vertified value (used as key for storage).
    #[serde(skip_serializing)]
    pub hash: HashValue,
    /// Signatures on the value.
    pub signatures: Vec<(AuthorityName, Signature)>,
}

/// Order to process a confirmed request.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ConfirmationOrder {
    pub certificate: Certificate,
}

/// Message to obtain information on an account.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct AccountInfoQuery {
    pub account_id: AccountId,
    pub query_sequence_number: Option<SequenceNumber>,
    pub query_received_certificates_excluding_first_nth: Option<usize>,
}

/// The response to an `AccountInfoQuery`
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct AccountInfoResponse {
    pub account_id: AccountId,
    pub manager: AccountManager,
    pub balance: Balance,
    pub next_sequence_number: SequenceNumber,
    pub count_received_certificates: usize,
    pub queried_certificate: Option<Certificate>,
    pub queried_received_certificates: Vec<Certificate>,
}

/// A (trusted) cross-shard request with an authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum CrossShardRequest {
    UpdateRecipient {
        certificate: Certificate,
    },
    DestroyAccount {
        account_id: AccountId,
    },
    ProcessConfirmedRequest {
        request: Request,
        certificate: Certificate,
    },
    ConfirmUpdatedRecipient {
        account_id: AccountId,
        hash: HashValue,
    },
}

impl CrossShardRequest {
    /// Where to send the cross-shard request.
    pub fn target_account_id(&self) -> &AccountId {
        use CrossShardRequest::*;
        match self {
            UpdateRecipient { certificate } => certificate
                .value
                .confirm_request()
                .unwrap()
                .operation
                .recipient()
                .unwrap(),
            DestroyAccount { account_id } => account_id,
            ProcessConfirmedRequest {
                request,
                certificate: _,
            } => &request.account_id,
            ConfirmUpdatedRecipient {
                account_id,
                hash: _,
            } => account_id,
        }
    }
}

impl Operation {
    pub fn recipient(&self) -> Option<&AccountId> {
        use Operation::*;
        match self {
            Transfer {
                recipient: Address::Account(id),
                ..
            }
            | OpenAccount { new_id: id, .. } => Some(id),
            _ => None,
        }
    }

    pub fn received_amount(&self) -> Option<Amount> {
        use Operation::*;
        match self {
            Transfer { amount, .. } => Some(*amount),
            _ => None,
        }
    }
}

impl Value {
    pub fn confirm_account_id(&self) -> Option<&AccountId> {
        match self {
            Value::Confirm(r) => Some(&r.account_id),
            _ => None,
        }
    }

    pub fn confirm_sequence_number(&self) -> Option<SequenceNumber> {
        match self {
            Value::Confirm(r) => Some(r.sequence_number),
            _ => None,
        }
    }

    pub fn confirm_request(&self) -> Option<&Request> {
        match self {
            Value::Confirm(r) => Some(r),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn confirm_request_mut(&mut self) -> Option<&mut Request> {
        match self {
            Value::Confirm(r) => Some(r),
            _ => None,
        }
    }

    pub fn confirm_key(&self) -> Option<(AccountId, SequenceNumber)> {
        match self {
            Value::Confirm(r) => Some((r.account_id.clone(), r.sequence_number)),
            _ => None,
        }
    }

    pub fn lock_account_id(&self) -> Option<&AccountId> {
        match self {
            Value::Lock(r) => Some(&r.account_id),
            _ => None,
        }
    }

    pub fn pre_commit_proposal(&self) -> Option<&ConsensusProposal> {
        match self {
            Value::PreCommit { proposal, .. } => Some(proposal),
            _ => None,
        }
    }
}

/// Non-testing code should make the pattern matching explicit so that
/// we know where to add protocols in the future.
#[cfg(test)]
impl Request {
    pub(crate) fn amount(&self) -> Option<Amount> {
        match &self.operation {
            Operation::Transfer { amount, .. } => Some(*amount),
            _ => None,
        }
    }
}

impl From<Request> for RequestValue {
    fn from(request: Request) -> Self {
        Self {
            request,
            limited_to: None,
        }
    }
}

impl RequestOrder {
    pub fn new(value: RequestValue, secret: &KeyPair) -> Self {
        let signature = Signature::new(&value, secret);
        Self {
            value,
            owner: secret.public(),
            signature,
        }
    }

    pub fn check(&self, authentication_method: Option<&AccountOwner>) -> Result<(), Error> {
        ensure!(
            authentication_method == Some(&self.owner),
            Error::InvalidOwner
        );
        self.signature.check(&self.value, self.owner)
    }
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: Value, key_pair: &KeyPair) -> Self {
        let signature = Signature::new(&value, key_pair);
        Self {
            value,
            authority: key_pair.public(),
            signature,
        }
    }

    /// Verify the signature and return the non-zero voting right of the authority.
    pub fn check(&self, committee: &Committee) -> Result<usize, Error> {
        let weight = committee.weight(&self.authority);
        ensure!(weight > 0, Error::UnknownSigner);
        self.signature.check(&self.value, self.authority)?;
        Ok(weight)
    }
}

pub struct SignatureAggregator<'a> {
    committee: &'a Committee,
    weight: usize,
    used_authorities: HashSet<AuthorityName>,
    partial: Certificate,
}

impl<'a> SignatureAggregator<'a> {
    /// Start aggregating signatures for the given value into a certificate.
    pub fn new(value: Value, committee: &'a Committee) -> Self {
        let hash = HashValue::new(&value);
        Self {
            committee,
            weight: 0,
            used_authorities: HashSet::new(),
            partial: Certificate {
                hash,
                value,
                signatures: Vec::new(),
            },
        }
    }

    /// Try to append a signature to a (partial) certificate. Returns Some(certificate) if a quorum was reached.
    /// The resulting final certificate is guaranteed to be valid in the sense of `check` below.
    /// Returns an error if the signed value cannot be aggregated.
    pub fn append(
        &mut self,
        authority: AuthorityName,
        signature: Signature,
    ) -> Result<Option<Certificate>, Error> {
        signature.check(&self.partial.value, authority)?;
        // Check that each authority only appears once.
        ensure!(
            !self.used_authorities.contains(&authority),
            Error::CertificateAuthorityReuse
        );
        self.used_authorities.insert(authority);
        // Update weight.
        let voting_rights = self.committee.weight(&authority);
        ensure!(voting_rights > 0, Error::UnknownSigner);
        self.weight += voting_rights;
        // Update certificate.
        self.partial.signatures.push((authority, signature));

        if self.weight >= self.committee.quorum_threshold() {
            self.weight = 0; // Prevent from creating the certificate twice.
            Ok(Some(self.partial.clone()))
        } else {
            Ok(None)
        }
    }
}

impl<'a> Deserialize<'a> for Certificate {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'a>,
    {
        #[derive(Deserialize)]
        #[serde(rename = "Certificate")]
        struct NetworkCertificate {
            value: Value,
            signatures: Vec<(AuthorityName, Signature)>,
        }

        let cert = NetworkCertificate::deserialize(deserializer)?;
        Ok(Certificate::new(cert.value, cert.signatures))
    }
}

impl Certificate {
    pub fn new(value: Value, signatures: Vec<(AuthorityName, Signature)>) -> Self {
        let hash = HashValue::new(&value);
        Self {
            value,
            hash,
            signatures,
        }
    }

    /// Verify the certificate.
    pub fn check<'a>(&'a self, committee: &Committee) -> Result<&'a Value, Error> {
        // Check the quorum.
        let mut weight = 0;
        let mut used_authorities = HashSet::new();
        for (authority, _) in self.signatures.iter() {
            // Check that each authority only appears once.
            ensure!(
                !used_authorities.contains(authority),
                Error::CertificateAuthorityReuse
            );
            used_authorities.insert(*authority);
            // Update weight.
            let voting_rights = committee.weight(authority);
            ensure!(voting_rights > 0, Error::UnknownSigner);
            weight += voting_rights;
        }
        ensure!(
            weight >= committee.quorum_threshold(),
            Error::CertificateRequiresQuorum
        );
        // All what is left is checking signatures!
        Signature::verify_batch(&self.value, &self.signatures)?;
        Ok(&self.value)
    }
}

impl ConfirmationOrder {
    pub fn new(certificate: Certificate) -> Self {
        Self { certificate }
    }
}

impl BcsSignable for RequestValue {}
impl BcsSignable for Value {}
impl BcsSignable for ConsensusProposal {}
