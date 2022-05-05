// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::{base_types::*, chain::ChainManager, committee::Committee, ensure, error::Error};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[cfg(test)]
#[path = "unit_tests/messages_tests.rs"]
mod messages_tests;

/// A recipient's address.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Address {
    Burn,             // for demo purposes
    Account(ChainId), // TODO: support several accounts per chain
}

/// An chain operation.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Operation {
    /// Transfer `amount` units of value to the recipient.
    Transfer {
        recipient: Address,
        amount: Amount,
        user_data: UserData,
    },
    /// Create (or activate) a new chain by installing the given authentication key.
    OpenChain { new_id: ChainId, new_owner: Owner },
    /// Close the chain.
    CloseChain,
    /// Change the authentication key of the chain.
    ChangeOwner { new_owner: Owner },
    /// Change the authentication key of the chain.
    ChangeMultipleOwners { new_owners: Vec<Owner> },
}

/// A block containing a single operation for the given chain.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct Block {
    /// The subject of the operation.
    pub chain_id: ChainId,
    /// The operation to execute.
    pub operation: Operation,
    /// The block height.
    pub height: BlockHeight,
    /// Certified hash (see `Certificate` below) of the previous block in the
    /// chain, if any.
    pub previous_block_hash: Option<HashValue>,
}

/// A block with a round number.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub struct BlockAndRound(pub Block, pub RoundNumber);

/// An authenticated proposal for a new block.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct BlockProposal {
    pub block_and_round: BlockAndRound,
    pub owner: Owner,
    pub signature: Signature,
}

/// A statement to be certified by the validators.
#[derive(Debug, PartialEq, Eq, Hash, Clone, Serialize, Deserialize)]
pub enum Value {
    /// The block was validated but confirmation will require additional steps.
    Validated { block: Block, round: RoundNumber },
    /// The block is validated and final (i.e. ready to be executed).
    Confirmed { block: Block },
}

/// A vote on a statement from a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct Vote {
    pub value: Value,
    pub validator: ValidatorName,
    pub signature: Signature,
}

/// A certified statement from the committee.
#[derive(Clone, Debug, Serialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct Certificate {
    /// The certified value.
    pub value: Value,
    /// Hash of the certified value (used as key for storage).
    #[serde(skip_serializing)]
    pub hash: HashValue,
    /// Signatures on the value.
    pub signatures: Vec<(ValidatorName, Signature)>,
}

/// A range of block heights as used in ChainInfoQuery.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct BlockHeightRange {
    /// Starting point
    pub start: BlockHeight,
    /// Optional limit on the number of elements.
    pub limit: Option<usize>,
}

/// Message to obtain information on a chain.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ChainInfoQuery {
    /// The chain id
    pub chain_id: ChainId,
    /// Optionally block that the block height is the one expected.
    pub check_next_block_height: Option<BlockHeight>,
    /// Query the current committee.
    pub query_committee: bool,
    /// Query a range of certificates sent from the chain.
    pub query_sent_certificates_in_range: Option<BlockHeightRange>,
    /// Query new certificates received from the chain.
    pub query_received_certificates_excluding_first_nth: Option<usize>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ChainInfo {
    /// The chain id
    pub chain_id: ChainId,
    /// The state of the chain authentication.
    pub manager: ChainManager,
    /// The current balance.
    pub balance: Balance,
    /// The last block hash, if any.
    pub block_hash: Option<HashValue>,
    /// The current block height
    pub next_block_height: BlockHeight,
    /// The current committee (if requested)
    pub queried_committee: Option<Committee>,
    /// The response to `query_sent_certificates_in_range`
    pub queried_sent_certificates: Vec<Certificate>,
    /// The current number of received certificates (useful for `query_received_certificates_excluding_first_nth`)
    pub count_received_certificates: usize,
    /// The response to `query_received_certificates_excluding_first_nth`
    pub queried_received_certificates: Vec<Certificate>,
}

/// The response to an `ChainInfoQuery`
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ChainInfoResponse {
    pub info: ChainInfo,
    pub signature: Option<Signature>,
}

/// A (trusted) cross-chain request with a validator.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
#[allow(clippy::large_enum_variant)]
pub enum CrossChainRequest {
    UpdateRecipient {
        committee: Committee, // TODO: This should be an epoch number.
        certificate: Certificate,
    },
    ConfirmUpdatedRecipient {
        chain_id: ChainId,
        hash: HashValue,
    },
}

impl CrossChainRequest {
    /// Where to send the cross-chain request.
    pub fn target_chain_id(&self) -> &ChainId {
        use CrossChainRequest::*;
        match self {
            UpdateRecipient { certificate, .. } => certificate
                .value
                .confirmed_block()
                .unwrap()
                .operation
                .recipient()
                .unwrap(),
            ConfirmUpdatedRecipient { chain_id, hash: _ } => chain_id,
        }
    }
}

impl Operation {
    pub fn recipient(&self) -> Option<&ChainId> {
        use Operation::*;
        match self {
            Transfer {
                recipient: Address::Account(id),
                ..
            }
            | OpenChain { new_id: id, .. } => Some(id),
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
    pub fn chain_id(&self) -> &ChainId {
        match self {
            Value::Confirmed { block } => &block.chain_id,
            Value::Validated { block, .. } => &block.chain_id,
        }
    }

    pub fn block(&self) -> &Block {
        match self {
            Value::Confirmed { block } => block,
            Value::Validated { block, .. } => block,
        }
    }

    #[cfg(test)]
    pub fn confirmed_block_height(&self) -> Option<BlockHeight> {
        match self {
            Value::Confirmed { block } => Some(block.height),
            _ => None,
        }
    }

    pub fn confirmed_block(&self) -> Option<&Block> {
        match self {
            Value::Confirmed { block } => Some(block),
            _ => None,
        }
    }

    pub fn validated_block(&self) -> Option<&Block> {
        match self {
            Value::Validated { block, .. } => Some(block),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn confirmed_block_mut(&mut self) -> Option<&mut Block> {
        match self {
            Value::Confirmed { block } => Some(block),
            _ => None,
        }
    }

    pub fn confirmed_key(&self) -> Option<(ChainId, BlockHeight)> {
        match self {
            Value::Confirmed { block } => Some((block.chain_id.clone(), block.height)),
            _ => None,
        }
    }
}

impl BlockProposal {
    pub fn new(block_and_round: BlockAndRound, secret: &KeyPair) -> Self {
        let signature = Signature::new(&block_and_round, secret);
        Self {
            block_and_round,
            owner: secret.public(),
            signature,
        }
    }

    // TODO: this API is not great
    pub fn check(&self, manager: &ChainManager) -> Result<(), Error> {
        ensure!(
            manager.is_active(),
            Error::InactiveChain(self.block_and_round.0.chain_id.clone())
        );
        ensure!(manager.has_owner(&self.owner), Error::InvalidOwner);
        self.signature.check(&self.block_and_round, self.owner)
    }
}

impl Vote {
    /// Use signing key to create a signed object.
    pub fn new(value: Value, key_pair: &KeyPair) -> Self {
        let signature = Signature::new(&value, key_pair);
        Self {
            value,
            validator: key_pair.public(),
            signature,
        }
    }

    /// Verify the signature in the vote.
    pub fn check(&self, name: ValidatorName) -> Result<(), Error> {
        self.signature.check(&self.value, name)
    }
}

impl ChainInfoResponse {
    pub fn new(info: ChainInfo, key_pair: Option<&KeyPair>) -> Self {
        let signature = key_pair.map(|kp| Signature::new(&info, kp));
        Self { info, signature }
    }

    pub fn check(&self, name: ValidatorName) -> Result<(), Error> {
        match self.signature {
            Some(sig) => sig.check(&self.info, name),
            None => Err(Error::InvalidChainInfoResponse),
        }
    }
}

pub struct SignatureAggregator<'a> {
    committee: &'a Committee,
    weight: usize,
    used_validators: HashSet<ValidatorName>,
    partial: Certificate,
}

impl<'a> SignatureAggregator<'a> {
    /// Start aggregating signatures for the given value into a certificate.
    pub fn new(value: Value, committee: &'a Committee) -> Self {
        let hash = HashValue::new(&value);
        Self {
            committee,
            weight: 0,
            used_validators: HashSet::new(),
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
        validator: ValidatorName,
        signature: Signature,
    ) -> Result<Option<Certificate>, Error> {
        signature.check(&self.partial.value, validator)?;
        // Check that each validator only appears once.
        ensure!(
            !self.used_validators.contains(&validator),
            Error::CertificateValidatorReuse
        );
        self.used_validators.insert(validator);
        // Update weight.
        let voting_rights = self.committee.weight(&validator);
        ensure!(voting_rights > 0, Error::InvalidSigner);
        self.weight += voting_rights;
        // Update certificate.
        self.partial.signatures.push((validator, signature));

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
            signatures: Vec<(ValidatorName, Signature)>,
        }

        let cert = NetworkCertificate::deserialize(deserializer)?;
        Ok(Certificate::new(cert.value, cert.signatures))
    }
}

impl Certificate {
    pub fn new(value: Value, signatures: Vec<(ValidatorName, Signature)>) -> Self {
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
        let mut used_validators = HashSet::new();
        for (validator, _) in self.signatures.iter() {
            // Check that each validator only appears once.
            ensure!(
                !used_validators.contains(validator),
                Error::CertificateValidatorReuse
            );
            used_validators.insert(*validator);
            // Update weight.
            let voting_rights = committee.weight(validator);
            ensure!(voting_rights > 0, Error::InvalidSigner);
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

impl BcsSignable for ChainInfo {}
impl BcsSignable for BlockAndRound {}
impl BcsSignable for Value {}
