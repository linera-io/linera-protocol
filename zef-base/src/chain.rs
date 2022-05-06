// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{base_types::*, committee::Committee, ensure, error::Error, messages::*};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// State of a chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct ChainState {
    /// The UID of the chain.
    pub id: ChainId,
    /// Execution state.
    pub state: ExecutionState,
    /// Hash of the latest certified block in this chain, if any.
    pub block_hash: Option<HashValue>,
    /// Sequence number tracking blocks.
    pub next_block_height: BlockHeight,

    /// Hashes of all certified blocks for this sender.
    /// This ends with `block_hash` and has length `usize::from(next_block_height)`.
    pub confirmed_log: Vec<HashValue>,
    /// Hashes of all certified blocks known as a receiver (local ordering).
    pub received_log: Vec<HashValue>,

    /// Mailboxes used to send messages, indexed by recipient.
    pub outboxes: HashMap<ChainId, OutboxState>,
    /// Mailboxes used to receive messages, indexed by sender.
    pub inboxes: HashMap<ChainId, InboxState>,
}

/// An outbox used to send messages to another chain. NOTE: Messages are implied by the
/// execution of blocks, so currently we just send the certified blocks over and let the
/// receivers figure out what was the message for them.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct OutboxState {
    /// Keep sending these certified blocks of ours until they are acknowledged by
    /// receivers. Keep the height around so that we can quickly dequeue.
    pub queue: VecDeque<(BlockHeight, HashValue)>,
}

/// Aninbox used to receive and execute messages from another chain.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct InboxState {
    /// We have already received the cross-chain requests and enqueued all the messages
    /// below this height.
    pub next_height_to_receive: BlockHeight,
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
    pub owner: Owner,
    /// Latest proposal that we have voted on last (both to validate and confirm it).
    pub pending: Option<Vote>,
}

/// The specific state of a chain managed by multiple owners.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[cfg_attr(test, derive(Eq, PartialEq))]
pub struct MultiOwnerManager {
    /// The co-owners of the chain.
    pub owners: HashSet<Owner>,
    /// Latest authenticated block that we have received.
    pub proposed: Option<BlockProposal>,
    /// Latest validated proposal that we have seen (and voted to confirm).
    pub locked: Option<Certificate>,
    /// Latest proposal that we have voted on (either to validate or to confirm it).
    pub pending: Option<Vote>,
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
    pub fn new(owner: Owner) -> Self {
        SingleOwnerManager {
            owner,
            pending: None,
        }
    }
}

impl MultiOwnerManager {
    pub fn new(owners: HashSet<Owner>) -> Self {
        MultiOwnerManager {
            owners,
            proposed: None,
            locked: None,
            pending: None,
        }
    }

    pub fn round(&self) -> RoundNumber {
        let mut current_round = RoundNumber::default();
        if let Some(proposal) = &self.proposed {
            if current_round < proposal.block_and_round.1 {
                current_round = proposal.block_and_round.1;
            }
        }
        if let Some(cert) = &self.locked {
            if let Value::Validated { round, .. } = &cert.value {
                if current_round < *round {
                    current_round = *round;
                }
            }
        }
        current_round
    }
}

impl ChainManager {
    pub fn single(owner: Owner) -> Self {
        ChainManager::Single(Box::new(SingleOwnerManager::new(owner)))
    }

    pub fn multiple(owners: HashSet<Owner>) -> Self {
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

    pub fn has_owner(&self, owner: &Owner) -> bool {
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

    /// Verify the safety of the block w.r.t. voting rules.
    pub fn check_proposed_block(
        &self,
        block_hash: Option<HashValue>,
        next_block_height: BlockHeight,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, Error> {
        ensure!(
            new_block.height <= BlockHeight::max(),
            Error::InvalidBlockHeight
        );
        ensure!(
            new_block.previous_block_hash == block_hash,
            Error::UnexpectedPreviousBlockHash
        );
        ensure!(
            new_block.height == next_block_height,
            Error::UnexpectedBlockHeight
        );
        match self {
            ChainManager::Single(manager) => {
                ensure!(
                    new_round == RoundNumber::default(),
                    Error::InvalidBlockProposal
                );
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Confirmed { block } if block != new_block => {
                            return Err(Error::PreviousBlockMustBeConfirmedFirst);
                        }
                        Value::Validated { .. } => {
                            return Err(Error::InvalidBlockProposal);
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            ChainManager::Multi(manager) => {
                if let Some(proposal) = &manager.proposed {
                    if proposal.block_and_round.0 == *new_block
                        && proposal.block_and_round.1 == new_round
                    {
                        return Ok(Outcome::Skip);
                    }
                    if new_round <= proposal.block_and_round.1 {
                        return Err(Error::InsufficientRound(proposal.block_and_round.1));
                    }
                }
                if let Some(cert) = &manager.locked {
                    match &cert.value {
                        Value::Validated { round, .. } if new_round <= *round => {
                            return Err(Error::InsufficientRound(*round));
                        }
                        Value::Validated { block, round } if new_block != block => {
                            return Err(Error::HasLockedBlock(block.height, *round));
                        }
                        _ => (),
                    }
                }
                Ok(Outcome::Accept)
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn check_validated_block(
        &self,
        next_block_height: BlockHeight,
        new_block: &Block,
        new_round: RoundNumber,
    ) -> Result<Outcome, Error> {
        if next_block_height < new_block.height {
            return Err(Error::MissingEarlierBlocks {
                current_block_height: next_block_height,
            });
        }
        if next_block_height > new_block.height {
            // Block was already confirmed.
            return Ok(Outcome::Skip);
        }
        match self {
            ChainManager::Multi(manager) => {
                if let Some(vote) = &manager.pending {
                    match &vote.value {
                        Value::Confirmed { block } if block == new_block => {
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
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_vote(&mut self, proposal: BlockProposal, key_pair: Option<&KeyPair>) {
        match self {
            ChainManager::Single(manager) => {
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let BlockAndRound(block, _) = proposal.block_and_round;
                    let value = Value::Confirmed { block };
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            ChainManager::Multi(manager) => {
                // Record the proposed block. This is important to keep track of rounds
                // for non-voting nodes.
                manager.proposed = Some(proposal.clone());
                if let Some(key_pair) = key_pair {
                    // Vote to validate.
                    let BlockAndRound(block, round) = proposal.block_and_round;
                    let value = Value::Validated { block, round };
                    let vote = Vote::new(value, key_pair);
                    manager.pending = Some(vote);
                }
            }
            _ => panic!("unexpected chain manager"),
        }
    }

    pub fn create_final_vote(
        &mut self,
        block: Block,
        certificate: Certificate,
        key_pair: Option<&KeyPair>,
    ) {
        match self {
            ChainManager::Multi(manager) => {
                // Record validity certificate. This is important to keep track of rounds
                // for non-voting nodes.
                manager.locked = Some(certificate);
                if let Some(key_pair) = key_pair {
                    // Vote to confirm.
                    let value = Value::Confirmed { block };
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
            block_hash: self.block_hash,
            next_block_height: self.next_block_height,
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
            block_hash: None,
            next_block_height: BlockHeight::new(),
            confirmed_log: Vec::new(),
            received_log: Vec::new(),
            inboxes: HashMap::new(),
            outboxes: HashMap::new(),
        }
    }

    pub fn create(committee: Committee, id: ChainId, owner: Owner, balance: Balance) -> Self {
        let mut chain = Self::new(id);
        chain.state.committee = Some(committee);
        chain.state.manager = ChainManager::single(owner);
        chain.state.balance = balance;
        chain
    }

    /// Verify that the operation is valid and return the value to certify.
    pub fn validate_operation(&self, block: &Block) -> Result<(), Error> {
        match &block.operation {
            Operation::Transfer { amount, .. } => {
                ensure!(*amount > Amount::zero(), Error::IncorrectTransferAmount);
                ensure!(
                    self.state.balance >= (*amount).into(),
                    Error::InsufficientFunding {
                        current_balance: self.state.balance
                    }
                );
            }
            Operation::OpenChain { id, committee, .. } => {
                let expected_id = block.chain_id.make_child(block.height);
                ensure!(id == &expected_id, Error::InvalidNewChainId(id.clone()));
                ensure!(
                    self.state.committee.as_ref() == Some(committee),
                    Error::InvalidCommittee
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

    /// Whether an invalid operation for this block can become valid later.
    pub fn is_retriable_validation_error(block: &Block, error: &Error) -> bool {
        match (&block.operation, error) {
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
    /// Operations must be executed by order of heights in the sender's chain.
    pub fn apply_operation_as_recipient(
        &mut self,
        operation: &Operation,
        key: HashValue,
        sender_id: ChainId,
        block_height: BlockHeight,
    ) -> Result<bool, Error> {
        let inbox = self.inboxes.entry(sender_id).or_default();
        if block_height < inbox.next_height_to_receive {
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
            Operation::OpenChain {
                owner, committee, ..
            } => {
                assert!(!self.state.manager.is_active()); // guaranteed under BFT assumptions.
                assert!(self.state.committee.is_none());
                self.state.committee = Some(committee.clone());
                self.state.manager = ChainManager::single(*owner);
            }
            _ => unreachable!("Not an operation with recipients"),
        }
        self.received_log.push(key);
        inbox.next_height_to_receive = block_height.try_add_one()?;
        Ok(true)
    }
}
