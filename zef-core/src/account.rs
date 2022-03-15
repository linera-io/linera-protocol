// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, ensure, error::Error, messages::*};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// State of an account.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct AccountState {
    /// The UID of the account.
    pub id: AccountId,
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
        new_request: &Request,
        new_round: Option<RoundNumber>,
    ) -> Result<Outcome, Error> {
        match self {
            AccountManager::Single(manager) => {
                ensure!(new_round.is_none(), Error::InvalidRequestOrder);
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
                let new_round = new_round.ok_or(Error::InvalidRequestOrder)?;
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Validated { request, round }
                            if *round == new_round && request == new_request =>
                        {
                            return Ok(Outcome::Skip);
                        }
                        Value::Validated { round, .. } if new_round <= *round => {
                            return Err(Error::InsufficientRound(*round));
                        }
                        _ => (),
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { round, .. } if new_round <= *round => {
                            return Err(Error::InsufficientRound(*round));
                        }
                        Value::Validated { request, .. } if new_request != request => {
                            return Err(Error::HasLockedRequest(request.clone()));
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
        new_request: &Request,
        new_round: RoundNumber,
    ) -> Result<Outcome, Error> {
        match self {
            AccountManager::Multi(manager) => {
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Confirmed { request } if request == new_request => {
                            return Ok(Outcome::Skip);
                        }
                        Value::Validated { round, .. } if new_round < *round => {
                            return Err(Error::InsufficientRound(round.try_sub_one().unwrap()));
                        }
                        _ => (),
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { round, .. } if new_round < *round => {
                            return Err(Error::InsufficientRound(round.try_sub_one().unwrap()));
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected account manager"),
        }
    }

    pub fn create_vote(&mut self, order: RequestOrder, key_pair: &KeyPair) {
        match self {
            AccountManager::Single(manager) => {
                // Vote to confirm
                let request = order.value.request;
                let value = Value::Confirmed { request };
                let vote = Vote::new(value, key_pair);
                manager.pending = Some(vote);
            }
            AccountManager::Multi(manager) => {
                // Record the user's authenticated request
                manager.order = Some(order.clone());
                // Vote to validate
                let round = order.value.round.expect("round was checked");
                let request = order.value.request;
                let value = Value::Validated { request, round };
                let vote = Vote::new(value, key_pair);
                manager.pending = Some(vote);
            }
            _ => panic!("unexpected account manager"),
        }
    }

    pub fn create_final_vote(
        &mut self,
        request: Request,
        certificate: Certificate,
        key_pair: &KeyPair,
    ) {
        match self {
            AccountManager::Multi(manager) => {
                // Record validity certificate.
                manager.locked = Some(certificate);
                // Vote to confirm
                let value = Value::Confirmed { request };
                let vote = Vote::new(value, key_pair);
                // Ok to overwrite validation votes with confirmation votes at equal or
                // higher round.
                manager.pending = Some(vote);
            }
            _ => panic!("unexpected account manager"),
        }
    }
}

impl AccountState {
    pub(crate) fn make_account_info(&self) -> AccountInfoResponse {
        AccountInfoResponse {
            account_id: self.id.clone(),
            manager: self.manager.clone(),
            balance: self.balance,
            next_sequence_number: self.next_sequence_number,
            count_received_certificates: self.received_log.len(),
            queried_certificate: None,
            queried_received_certificates: Vec::new(),
        }
    }

    pub fn new(id: AccountId) -> Self {
        Self {
            id,
            manager: AccountManager::None,
            balance: Balance::default(),
            next_sequence_number: SequenceNumber::new(),
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            keep_sending: HashSet::new(),
            received_keys: HashSet::new(),
        }
    }

    pub fn create(id: AccountId, owner: AccountOwner, balance: Balance) -> Self {
        let mut account = Self::new(id);
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
        key: HashValue,
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
                self.manager = AccountManager::single(*new_owner);
            }
            _ => unreachable!("Not an operation with recipients"),
        }
        self.received_keys.insert(key);
        self.received_log.push(key);
        Ok(true)
    }
}
