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
    /// Whether we have signed a request for this sequence number already.
    pub pending: Option<Vote>,
}

/// The specific state of an account managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct MultiOwnerManager {
    /// The co-owners of the account.
    pub owners: HashSet<AccountOwner>,
    /// Pending proposal that we have validated and perhaps locked (i.e. voted to be final).
    pub pending: Option<Vote>,
    /// Validation certificate of a proposal that we have locked.
    pub locked: Option<Certificate>,
}

impl Default for AccountManager {
    fn default() -> Self {
        AccountManager::None
    }
}

impl AccountManager {
    pub fn single(owner: AccountOwner) -> Self {
        AccountManager::Single(Box::new(SingleOwnerManager {
            owner,
            pending: None,
        }))
    }

    pub fn reset(&mut self) {
        match self {
            AccountManager::None => (),
            AccountManager::Single(manager) => {
                manager.pending = None;
            }
            AccountManager::Multi(manager) => {
                manager.pending = None;
                manager.locked = None;
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
    /// Returns true if the same request has been handled already.
    pub fn check_pending_request(
        &self,
        new_request: &Request,
        new_round: Option<SequenceNumber>,
    ) -> Result<bool, Error> {
        match self {
            AccountManager::Single(manager) => match &manager.pending {
                Some(pending) => {
                    ensure!(
                        matches!(&pending.value, Value::Confirmed(request) if request == new_request),
                        Error::PreviousRequestMustBeConfirmedFirst
                    );
                    ensure!(new_round.is_none(), Error::InvalidRequestOrder);
                    Ok(true)
                }
                None => Ok(false),
            },
            AccountManager::Multi(manager) => {
                let new_round = new_round.ok_or(Error::InvalidRequestOrder)?;
                if let Some(pending) = &manager.pending {
                    match &pending.value {
                        Value::Validated { request, round }
                            if *round == new_round && request == new_request =>
                        {
                            return Ok(true);
                        }
                        Value::Validated { round, .. } if *round < new_round => (),
                        Value::Confirmed(_) => {
                            // Verification continues below.
                            assert!(manager.locked.is_some());
                        }
                        _ => {
                            return Err(Error::InvalidRequestOrder);
                        }
                    }
                }
                if let Some(locked) = &manager.locked {
                    ensure!(
                        matches!(&locked.value, Value::Validated { request, round } if *round < new_round && request == new_request),
                        Error::InvalidRequestOrder
                    );
                }
                Ok(false)
            }
            _ => panic!("unexpected account manager"),
        }
    }

    pub fn create_vote(
        &mut self,
        request: Request,
        round: Option<SequenceNumber>,
        key_pair: &KeyPair,
    ) {
        match self {
            AccountManager::Single(manager) => {
                let value = Value::Confirmed(request);
                let vote = Vote::new(value, key_pair);
                manager.pending = Some(vote);
            }
            AccountManager::Multi(manager) => {
                let round = round.unwrap();
                let value = Value::Validated { request, round };
                let vote = Vote::new(value, key_pair);
                manager.pending = Some(vote);
            }
            _ => panic!("unexpected account manager"),
        }
    }

    /// Returns true if the same request has been handled already.
    pub fn check_validated_request(
        &self,
        new_request: &Request,
        new_round: SequenceNumber,
    ) -> Result<bool, Error> {
        match self {
            AccountManager::Multi(manager) => {
                if let Some(pending) = &manager.pending {
                    match &pending.value {
                        Value::Confirmed(request) if request == new_request => {
                            return Ok(true);
                        }
                        Value::Validated { round, .. } if *round <= new_round => (),
                        Value::Confirmed(_) => {
                            // Verification continues below.
                            assert!(manager.locked.is_some());
                        }
                        _ => {
                            return Err(Error::InvalidConfirmationOrder);
                        }
                    }
                }
                if let Some(locked) = &manager.locked {
                    ensure!(
                        matches!(&locked.value, Value::Validated { round, .. } if *round <= new_round),
                        Error::InvalidConfirmationOrder
                    );
                }
                Ok(false)
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
                let value = Value::Confirmed(request);
                let vote = Vote::new(value, key_pair);
                // Record confirmation.
                manager.locked = Some(certificate);
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
            Operation::CloseAccount | Operation::ChangeOwner { .. } => {
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
