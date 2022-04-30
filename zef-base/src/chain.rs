// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, committee::Committee, ensure, error::Error, messages::*};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// State of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ChainState {
    /// The UID of the chain.
    pub id: ChainId,
    /// Execution state.
    pub state: ExecutionState,
    /// Sequence number tracking requests.
    pub next_sequence_number: SequenceNumber,
    /// Hashes of all confirmed certificates for this sender.
    pub confirmed_log: Vec<HashValue>,
    /// Hashes of all confirmed certificates as a receiver.
    pub received_log: Vec<HashValue>,
    /// Maximum sequence number of all received updates indexed by sender.
    /// This is needed for clients.
    pub received_index: HashMap<ChainId, SequenceNumber>,

    /// Keep sending these confirmed certificates until they are confirmed by receivers.
    pub keep_sending: HashSet<HashValue>,
    /// Same as received_log but used for deduplication.
    pub received_keys: HashSet<HashValue>,
}

/// Execution state of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ExecutionState {
    /// Current committee, if any.
    pub committee: Option<Committee>,
    /// Manager of the chain.
    pub manager: ChainManager,
    /// Balance of the chain.
    pub balance: Balance,
}

/// How to produce new blocks.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum ChainManager {
    /// The chain is not active. (No blocks can be created)
    None,
    /// The chain is managed by a single owner.
    Single(Box<SingleOwnerManager>),
    /// The chain is managed by multiple owners.
    Multi(Box<MultiOwnerManager>),
}

/// The specific state of a chain managed by one owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct SingleOwnerManager {
    /// The owner of the chain.
    pub owner: ChainOwner,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

/// The specific state of a chain managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct MultiOwnerManager {
    /// The co-owners of the chain.
    pub owners: HashSet<ChainOwner>,
    /// Latest authenticated request that we have received.
    pub order: Option<RequestOrder>,
    /// Latest proposal that we have voted on (either to validate or to confirm it).
    pub pending: Option<Vote>,
    /// Latest validated proposal that we have seen (and voted to confirm).
    pub locked: Option<Certificate>,
}

/// The result of verifying a (valid) query.
#[derive(Eq, PartialEq)]
pub enum Outcome {
    Accept,
    Skip,
}

impl Default for ChainManager {
    fn default() -> Self {
        ChainManager::None
    }
}

impl SingleOwnerManager {
    pub fn new(owner: ChainOwner) -> Self {
        SingleOwnerManager {
            owner,
            pending: None,
        }
    }
}

impl MultiOwnerManager {
    pub fn new(owners: HashSet<ChainOwner>) -> Self {
        MultiOwnerManager {
            owners,
            order: None,
            pending: None,
            locked: None,
        }
    }

    pub fn round(&self) -> RoundNumber {
        let mut round = RoundNumber::default();
        if let Some(vote) = &self.pending {
            if let Value::Validated { request } = &vote.value {
                if round < request.round {
                    round = request.round;
                }
            }
        }
        if let Some(cert) = &self.locked {
            if let Value::Validated { request } = &cert.value {
                if round < request.round {
                    round = request.round;
                }
            }
        }
        round
    }
}

impl ChainManager {
    pub fn single(owner: ChainOwner) -> Self {
        ChainManager::Single(Box::new(SingleOwnerManager::new(owner)))
    }

    pub fn multiple(owners: HashSet<ChainOwner>) -> Self {
        ChainManager::Multi(Box::new(MultiOwnerManager::new(owners)))
    }

    pub fn reset(&mut self) {
        match self {
            ChainManager::None => (),
            ChainManager::Single(manager) => {
                *manager = Box::new(SingleOwnerManager::new(manager.owner));
            }
            ChainManager::Multi(manager) => {
                let owners = std::mem::take(&mut manager.owners);
                *manager = Box::new(MultiOwnerManager::new(owners));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, ChainManager::None)
    }

    pub fn has_owner(&self, owner: &ChainOwner) -> bool {
        match self {
            ChainManager::Single(manager) => manager.owner == *owner,
            ChainManager::Multi(manager) => manager.owners.contains(owner),
            ChainManager::None => false,
        }
    }

    pub fn next_round(&self) -> RoundNumber {
        match self {
            ChainManager::Multi(m) => {
                let round = m.round();
                round.try_add_one().unwrap_or(round)
            }
            _ => RoundNumber::default(),
        }
    }

    pub fn pending(&self) -> Option<&Vote> {
        match self {
            ChainManager::Single(manager) => manager.pending.as_ref(),
            ChainManager::Multi(manager) => manager.pending.as_ref(),
            _ => None,
        }
    }

    /// Verify the safety of the request w.r.t. voting rules.
    pub fn check_request(
        &self,
        next_sequence_number: SequenceNumber,
        new_request: &Request,
    ) -> Result<Outcome, Error> {
        ensure!(
            new_request.sequence_number <= SequenceNumber::max(),
            Error::InvalidSequenceNumber
        );
        ensure!(
            new_request.sequence_number == next_sequence_number,
            Error::UnexpectedSequenceNumber
        );
        match self {
            ChainManager::Single(manager) => {
                ensure!(
                    new_request.round == RoundNumber::default(),
                    Error::InvalidRequestOrder
                );
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Confirmed { request } if request != new_request => {
                            return Err(Error::PreviousRequestMustBeConfirmedFirst);
                        }
                        Value::Validated { .. } => {
                            return Err(Error::InvalidRequestOrder);
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            ChainManager::Multi(manager) => {
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Validated { request } if request == new_request => {
                            return Ok(Outcome::Skip);
                        }
                        Value::Validated { request } if new_request.round <= request.round => {
                            return Err(Error::InsufficientRound(request.round));
                        }
                        _ => (),
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { request } if new_request.round <= request.round => {
                            return Err(Error::InsufficientRound(request.round));
                        }
                        Value::Validated { request }
                            if new_request.operation != request.operation =>
                        {
                            return Err(Error::HasLockedOperation(request.operation.clone()));
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn check_validated_request(
        &self,
        next_sequence_number: SequenceNumber,
        new_request: &Request,
    ) -> Result<Outcome, Error> {
        if next_sequence_number < new_request.sequence_number {
            return Err(Error::MissingEarlierConfirmations {
                current_sequence_number: next_sequence_number,
            });
        }
        if next_sequence_number > new_request.sequence_number {
            // Request was already confirmed.
            return Ok(Outcome::Skip);
        }
        match self {
            ChainManager::Multi(manager) => {
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Confirmed { request } if request == new_request => {
                            return Ok(Outcome::Skip);
                        }
                        Value::Validated { request } if new_request.round < request.round => {
                            return Err(Error::InsufficientRound(
                                request.round.try_sub_one().unwrap(),
                            ));
                        }
                        _ => (),
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { request } if new_request.round < request.round => {
                            return Err(Error::InsufficientRound(
                                request.round.try_sub_one().unwrap(),
                            ));
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_vote(&mut self, order: RequestOrder, key_pair: Option<&KeyPair>) {
        match self {
            ChainManager::Single(manager) => {
                if let Some(key_pair) = key_pair {
                    // Vote to confirm
                    let request = order.request;
                    let value = Value::Confirmed { request };
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            ChainManager::Multi(manager) => {
                // Record the user's authenticated request
                manager.order = Some(order.clone());
                if let Some(key_pair) = key_pair {
                    // Vote to validate
                    let request = order.request;
                    let value = Value::Validated { request };
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_final_vote(
        &mut self,
        request: Request,
        certificate: Certificate,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            ChainManager::Multi(manager) => {
                // Record validity certificate.
                manager.locked = Some(certificate);
                if let Some(key_pair) = key_pair {
                    // Vote to confirm
                    let value = Value::Confirmed { request };
                    let vote = Vote::new(value, key_pair);
                    // Ok to overwrite validation votes with confirmation votes at equal or
                    // higher round.
                    manager.pending = Some(vote);
                }
            }
            _ => panic!("unexpected chain manager"),
        }
    }
}

impl ChainState {
    pub fn make_chain_info(&self, key_pair: Option<&KeyPair>) -> ChainInfoResponse {
        let info = ChainInfo {
            chain_id: self.id.clone(),
            manager: self.state.manager.clone(),
            balance: self.state.balance,
            queried_committee: None,
            next_sequence_number: self.next_sequence_number,
            queried_sent_certificates: Vec::new(),
            count_received_certificates: self.received_log.len(),
            queried_received_certificates: Vec::new(),
        };
        ChainInfoResponse::new(info, key_pair)
    }

    pub fn new(id: ChainId) -> Self {
        let state = ExecutionState {
            committee: None,
            manager: ChainManager::None,
            balance: Balance::default(),
        };
        Self {
            id,
            state,
            next_sequence_number: SequenceNumber::new(),
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            received_index: HashMap::new(),
            keep_sending: HashSet::new(),
            received_keys: HashSet::new(),
        }
    }

    pub fn create(committee: Committee, id: ChainId, owner: ChainOwner, balance: Balance) -> Self {
        let mut chain = Self::new(id);
        chain.state.committee = Some(committee);
        chain.state.manager = ChainManager::single(owner);
        chain.state.balance = balance;
        chain
    }

    /// Verify that the operation is valid and return the value to certify.
    pub fn validate_operation(&self, request: &Request) -> Result<(), Error> {
        match &request.operation {
            Operation::Transfer { amount, .. } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.state.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.state.balance
                    }
                );
            }
            Operation::OpenChain { new_id, .. } => {
                let expected_id = request.chain_id.make_child(request.sequence_number);
                ensure!(
                    new_id == &expected_id,
                    Error::InvalidNewChainId(new_id.clone())
                );
            }
            Operation::CloseChain
            | Operation::ChangeOwner { .. }
            | Operation::ChangeMultipleOwners { .. } => {
                // Nothing to check.
            }
        }
        Ok(())
    }

    /// Whether an invalid operation for this request can become valid later.
    pub fn is_retriable_validation_error(request: &Request, error: &Error) -> bool {
        match (&request.operation, error) {
            (Operation::Transfer { .. }, Error::InsufficientFunding { .. }) => true,
            (Operation::Transfer { .. }, _) => false,
            (
                Operation::OpenChain { .. }
                | Operation::CloseChain
                | Operation::ChangeOwner { .. }
                | Operation::ChangeMultipleOwners { .. },
                _,
            ) => false,
        }
    }

    /// Execute the sender's side of the operation.
    pub fn apply_operation_as_sender(
        &mut self,
        operation: &Operation,
        key: HashValue,
    ) -> Result<(), Error> {
        match operation {
            Operation::OpenChain { .. } => (),
            Operation::ChangeOwner { new_owner } => {
                self.state.manager = ChainManager::single(*new_owner);
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.state.manager = ChainManager::multiple(new_owners.iter().cloned().collect());
            }
            Operation::CloseChain => {
                self.state.manager = ChainManager::default();
            }
            Operation::Transfer { amount, .. } => {
                self.state.balance.try_sub_assign((*amount).into())?;
            }
        };
        self.confirmed_log.push(key);
        Ok(())
    }

    /// Execute the recipient's side of an operation.
    /// Returns true if the operation changed the chain state.
    pub fn apply_operation_as_recipient(
        &mut self,
        operation: &Operation,
        committee: Committee,
        // The rest is for logging purposes.
        key: HashValue,
        sender_id: ChainId,
        sequence_number: SequenceNumber,
    ) -> Result<bool, Error> {
        if self.received_keys.contains(&key) {
            // Confirmation already happened.
            return Ok(false);
        }
        match operation {
            Operation::Transfer { amount, .. } => {
                self.state.balance = self
                    .state
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
            }
            Operation::OpenChain { new_owner, .. } => {
                assert!(!self.state.manager.is_active()); // guaranteed under BFT assumptions.
                assert!(self.state.committee.is_none());
                self.state.committee = Some(committee);
                self.state.manager = ChainManager::single(*new_owner);
            }
            _ => unreachable!("Not an operation with recipients"),
        }
        self.received_keys.insert(key);
        self.received_log.push(key);
        let current = self
            .received_index
            .entry(sender_id)
            .or_insert(sequence_number);
        if sequence_number > *current {
            *current = sequence_number;
        }
        Ok(true)
    }
}
