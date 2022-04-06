// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, committee::Committee, ensure, error::Error, messages::*};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// State of an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct AccountState {
    /// The UID of the account.
    pub id: AccountId,
    /// Current committee, if any.
    pub committee: Option<Committee>,
    /// Manager of the account.
    pub manager: AccountManager,
    /// Balance of the account.
    pub balance: Balance,
    /// Sequence number tracking requests.
    pub next_sequence_number: SequenceNumber,
    /// Hashes of all confirmed certificates for this sender.
    pub confirmed_log: Vec<HashValue>,
    /// Hashes of all confirmed certificates as a receiver.
    pub received_log: Vec<HashValue>,
    /// Maximum sequence number of all received updates indexed by sender.
    /// This is needed for clients.
    pub received_index: HashMap<AccountId, SequenceNumber>,

    /// Keep sending these confirmed certificates until they are confirmed by receivers.
    pub keep_sending: HashSet<HashValue>,
    /// Same as received_log but used for deduplication.
    pub received_keys: HashSet<HashValue>,
}

/// How to produce new commands.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub enum AccountManager {
    /// The account is not active. (No blocks can be created)
    None,
    /// The account is managed by a single owner.
    Single(Box<SingleOwnerManager>),
    /// The account is managed by multiple owners.
    Multi(Box<MultiOwnerManager>),
}

/// The specific state of an account managed by one owner.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct SingleOwnerManager {
    /// The owner of the account.
    pub owner: AccountOwner,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

/// The specific state of an account managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct MultiOwnerManager {
    /// The co-owners of the account.
    pub owners: HashSet<AccountOwner>,
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

impl Default for AccountManager {
    fn default() -> Self {
        AccountManager::None
    }
}

impl SingleOwnerManager {
    pub fn new(owner: AccountOwner) -> Self {
        SingleOwnerManager {
            owner,
            pending: None,
        }
    }
}

impl MultiOwnerManager {
    pub fn new(owners: HashSet<AccountOwner>) -> Self {
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

impl AccountManager {
    pub fn single(owner: AccountOwner) -> Self {
        AccountManager::Single(Box::new(SingleOwnerManager::new(owner)))
    }

    pub fn multiple(owners: HashSet<AccountOwner>) -> Self {
        AccountManager::Multi(Box::new(MultiOwnerManager::new(owners)))
    }

    pub fn reset(&mut self) {
        match self {
            AccountManager::None => (),
            AccountManager::Single(manager) => {
                *manager = Box::new(SingleOwnerManager::new(manager.owner));
            }
            AccountManager::Multi(manager) => {
                let owners = std::mem::take(&mut manager.owners);
                *manager = Box::new(MultiOwnerManager::new(owners));
            }
        }
    }

    pub fn is_active(&self) -> bool {
        !matches!(self, AccountManager::None)
    }

    pub fn has_owner(&self, owner: &AccountOwner) -> bool {
        match self {
            AccountManager::Single(manager) => manager.owner == *owner,
            AccountManager::Multi(manager) => manager.owners.contains(owner),
            AccountManager::None => false,
        }
    }

    pub fn pending(&self) -> Option<&Vote> {
        match self {
            AccountManager::Single(manager) => manager.pending.as_ref(),
            AccountManager::Multi(manager) => manager.pending.as_ref(),
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
            AccountManager::Single(manager) => {
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
            AccountManager::Multi(manager) => {
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
            _ => panic!("unexpected account manager"),
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
            AccountManager::Multi(manager) => {
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
            _ => panic!("unexpected account manager"),
        }
    }

    pub fn create_vote(&mut self, order: RequestOrder, key_pair: Option<&KeyPair>) {
        match self {
            AccountManager::Single(manager) => {
                if let Some(key_pair) = key_pair {
                    // Vote to confirm
                    let request = order.request;
                    let value = Value::Confirmed { request };
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            AccountManager::Multi(manager) => {
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
            _ => panic!("unexpected account manager"),
        }
    }

    pub fn create_final_vote(
        &mut self,
        request: Request,
        certificate: Certificate,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            AccountManager::Multi(manager) => {
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
            _ => panic!("unexpected account manager"),
        }
    }
}

impl AccountState {
    pub(crate) fn make_account_info(&self, key_pair: Option<&KeyPair>) -> AccountInfoResponse {
        let info = AccountInfo {
            account_id: self.id.clone(),
            manager: self.manager.clone(),
            balance: self.balance,
            next_sequence_number: self.next_sequence_number,
            queried_sent_certificates: Vec::new(),
            count_received_certificates: self.received_log.len(),
            queried_received_certificates: Vec::new(),
        };
        AccountInfoResponse::new(info, key_pair)
    }

    pub fn new(id: AccountId) -> Self {
        Self {
            id,
            committee: None,
            manager: AccountManager::None,
            balance: Balance::default(),
            next_sequence_number: SequenceNumber::new(),
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            received_index: HashMap::new(),
            keep_sending: HashSet::new(),
            received_keys: HashSet::new(),
        }
    }

    pub fn create(
        committee: Committee,
        id: AccountId,
        owner: AccountOwner,
        balance: Balance,
    ) -> Self {
        let mut account = Self::new(id);
        account.committee = Some(committee);
        account.manager = AccountManager::single(owner);
        account.balance = balance;
        account
    }

    /// Verify that the operation is valid and return the value to certify.
    pub(crate) fn validate_operation(&self, request: &Request) -> Result<(), Error> {
        match &request.operation {
            Operation::Transfer { amount, .. } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.balance
                    }
                );
            }
            Operation::OpenAccount { new_id, .. } => {
                let expected_id = request.account_id.make_child(request.sequence_number);
                ensure!(
                    new_id == &expected_id,
                    Error::InvalidNewAccountId(new_id.clone())
                );
            }
            Operation::CloseAccount
            | Operation::ChangeOwner { .. }
            | Operation::ChangeMultipleOwners { .. } => {
                // Nothing to check.
            }
        }
        Ok(())
    }

    /// Whether an invalid operation for this request can become valid later.
    pub(crate) fn is_retriable_validation_error(request: &Request, error: &Error) -> bool {
        match (&request.operation, error) {
            (Operation::Transfer { .. }, Error::InsufficientFunding { .. }) => true,
            (Operation::Transfer { .. }, _) => false,
            (
                Operation::OpenAccount { .. }
                | Operation::CloseAccount
                | Operation::ChangeOwner { .. }
                | Operation::ChangeMultipleOwners { .. },
                _,
            ) => false,
        }
    }

    /// Execute the sender's side of the operation.
    pub(crate) fn apply_operation_as_sender(
        &mut self,
        operation: &Operation,
        key: HashValue,
    ) -> Result<(), Error> {
        match operation {
            Operation::OpenAccount { .. } => (),
            Operation::ChangeOwner { new_owner } => {
                self.manager = AccountManager::single(*new_owner);
            }
            Operation::ChangeMultipleOwners { new_owners } => {
                self.manager = AccountManager::multiple(new_owners.iter().cloned().collect());
            }
            Operation::CloseAccount => {
                self.manager = AccountManager::default();
            }
            Operation::Transfer { amount, .. } => {
                self.balance.try_sub_assign((*amount).into())?;
            }
        };
        self.confirmed_log.push(key);
        Ok(())
    }

    /// Execute the recipient's side of an operation.
    /// Returns true if the operation changed the account state.
    pub(crate) fn apply_operation_as_recipient(
        &mut self,
        operation: &Operation,
        committee: Committee,
        // The rest is for logging purposes.
        key: HashValue,
        sender_id: AccountId,
        sequence_number: SequenceNumber,
    ) -> Result<bool, Error> {
        if self.received_keys.contains(&key) {
            // Confirmation already happened.
            return Ok(false);
        }
        match operation {
            Operation::Transfer { amount, .. } => {
                self.balance = self
                    .balance
                    .try_add((*amount).into())
                    .unwrap_or_else(|_| Balance::max());
            }
            Operation::OpenAccount { new_owner, .. } => {
                assert!(!self.manager.is_active()); // guaranteed under BFT assumptions.
                assert!(self.committee.is_none());
                self.committee = Some(committee);
                self.manager = AccountManager::single(*new_owner);
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
