// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::AccountState, base_types::*, committee::Committee, consensus::ConsensusState, ensure,
    error::Error, messages::*,
};
use std::collections::BTreeMap;

#[cfg(test)]
#[path = "unit_tests/authority_tests.rs"]
mod authority_tests;

/// State of an authority.
pub struct AuthorityState {
    /// The name of this authority.
    pub name: AuthorityName,
    /// Committee of this instance.
    pub committee: Committee,
    /// The signature key pair of the authority.
    pub key_pair: KeyPair,
    /// Account states.
    pub accounts: BTreeMap<AccountId, AccountState>,
    /// States of consensus instances.
    pub instances: BTreeMap<AccountId, ConsensusState>,
    /// The sharding ID of this authority shard. 0 if one shard.
    pub shard_id: ShardId,
    /// The number of shards. 1 if single shard.
    pub number_of_shards: u32,
}

/// Interface provided by each (shard of an) authority.
/// All commands return either the current account info or an error.
/// Repeating commands produces no changes and returns no error.
pub trait Authority {
    /// Initiate a new request.
    fn handle_request_order(&mut self, order: RequestOrder) -> Result<AccountInfoResponse, Error>;

    /// Confirm a request.
    fn handle_confirmation_order(
        &mut self,
        order: ConfirmationOrder,
    ) -> Result<(AccountInfoResponse, CrossShardContinuation), Error>;

    /// Process a message meant for a consensus instance.
    fn handle_consensus_order(&mut self, order: ConsensusOrder)
        -> Result<ConsensusResponse, Error>;

    /// Handle information queries for this account.
    fn handle_account_info_query(
        &self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error>;

    /// Handle (trusted!) cross shard request.
    fn handle_cross_shard_request(&mut self, request: CrossShardRequest) -> Result<(), Error>;
}

impl AuthorityState {
    /// (Trusted) Process a confirmed request issued from an account.
    fn process_confirmed_request(
        &mut self,
        request: Request,
        certificate: Certificate, // For logging purpose
    ) -> Result<(AccountInfoResponse, CrossShardContinuation), Error> {
        // Verify sharding.
        ensure!(self.in_shard(&request.account_id), Error::WrongShard);
        // Obtain the sender's account.
        let sender = request.account_id.clone();
        let account = self
            .accounts
            .get_mut(&sender)
            .ok_or_else(|| Error::InactiveAccount(sender.clone()))?;
        // Check that the account is active and ready for this confirmation.
        ensure!(
            account.owner.is_some(),
            Error::InactiveAccount(sender.clone())
        );
        if account.next_sequence_number < request.sequence_number {
            return Err(Error::MissingEarlierConfirmations {
                current_sequence_number: account.next_sequence_number,
            });
        }
        if account.next_sequence_number > request.sequence_number {
            // Request was already confirmed.
            let info = account.make_account_info(sender.clone());
            return Ok((info, CrossShardContinuation::Done));
        }

        // Execute the sender's side of the operation.
        account.apply_operation_as_sender(&request.operation, certificate.clone())?;
        // Advance to next sequence number.
        account.next_sequence_number.try_add_assign_one()?;
        account.pending = None;
        // Final touch on the sender's account.
        let info = account.make_account_info(sender.clone());
        if account.owner.is_none() {
            // Tentatively remove inactive account. (It might be created again as a
            // recipient, though. To solve this, we may implement additional cleanups in
            // the future.)
            self.accounts.remove(&sender);
        }

        if let Some(recipient) = request.operation.recipient() {
            // Update recipient.
            if self.in_shard(recipient) {
                // Execute the operation locally.
                self.update_recipient_account(request.operation.clone(), certificate)?;
            } else {
                // Initiate a cross-shard request.
                let shard_id = self.which_shard(recipient);
                let cont = CrossShardContinuation::Request {
                    shard_id,
                    request: Box::new(CrossShardRequest::UpdateRecipient { certificate }),
                };
                return Ok((info, cont));
            }
        }
        Ok((info, CrossShardContinuation::Done))
    }

    /// (Trusted) Try to update the recipient account in a confirmed request.
    fn update_recipient_account(
        &mut self,
        operation: Operation,
        certificate: Certificate,
    ) -> Result<(), Error> {
        if let Some(recipient) = operation.recipient() {
            ensure!(self.in_shard(recipient), Error::WrongShard);
            // Execute the recipient's side of the operation.
            let account = self.accounts.entry(recipient.clone()).or_default();
            account.apply_operation_as_recipient(&operation, certificate)?;
        } else if let Operation::StartConsensusInstance {
            new_id,
            functionality: Functionality::AtomicSwap { accounts },
        } = &operation
        {
            ensure!(self.in_shard(new_id), Error::WrongShard);
            assert!(!self.instances.contains_key(new_id)); // guaranteed under BFT assumptions.
            let instance = ConsensusState::new(accounts.clone(), certificate);
            self.instances.insert(new_id.clone(), instance);
        }
        // This concludes the confirmation of `certificate`.
        Ok(())
    }
}

impl Authority for AuthorityState {
    fn handle_request_order(&mut self, order: RequestOrder) -> Result<AccountInfoResponse, Error> {
        // Verify sharding.
        ensure!(
            self.in_shard(&order.value.request.account_id),
            Error::WrongShard
        );
        // Verify that the order was meant for this authority.
        if let Some(authority) = &order.value.limited_to {
            ensure!(self.name == *authority, Error::InvalidRequestOrder);
        }
        // Obtain the sender's account.
        let sender = order.value.request.account_id.clone();

        let account = self
            .accounts
            .get_mut(&sender)
            .ok_or_else(|| Error::InactiveAccount(sender.clone()))?;
        ensure!(account.owner.is_some(), Error::InactiveAccount(sender));
        // Check authentication of the request.
        order.check(&account.owner)?;
        // Check the account is ready for this new request.
        let request = order.value.request;
        ensure!(
            request.sequence_number <= SequenceNumber::max(),
            Error::InvalidSequenceNumber
        );
        ensure!(
            account.next_sequence_number == request.sequence_number,
            Error::UnexpectedSequenceNumber
        );
        if let Some(pending) = &account.pending {
            ensure!(
                matches!(&pending.value, Value::Confirm(r) | Value::Lock(r) if r == &request),
                Error::PreviousRequestMustBeConfirmedFirst
            );
            // This exact request was already signed. Return the previous vote.
            return Ok(account.make_account_info(sender));
        }
        // Verify that the request is safe, and return the value of the vote.
        let value = account.validate_operation(request)?;
        let vote = Vote::new(value, &self.key_pair);
        account.pending = Some(vote);
        Ok(account.make_account_info(sender))
    }

    /// Confirm a request.
    fn handle_confirmation_order(
        &mut self,
        confirmation_order: ConfirmationOrder,
    ) -> Result<(AccountInfoResponse, CrossShardContinuation), Error> {
        // Verify that the certified value is a confirmation.
        let certificate = confirmation_order.certificate;
        let request = certificate
            .value
            .confirm_request()
            .ok_or(Error::InvalidConfirmationOrder)?;
        // Verify the certificate.
        certificate.check(&self.committee)?;
        // Process the request.
        self.process_confirmed_request(request.clone(), certificate)
    }

    /// Process a message meant for a consensus instance.
    fn handle_consensus_order(
        &mut self,
        order: ConsensusOrder,
    ) -> Result<ConsensusResponse, Error> {
        match order {
            ConsensusOrder::GetStatus { instance_id } => {
                let instance = self
                    .instances
                    .get_mut(&instance_id)
                    .ok_or(Error::UnknownConsensusInstance(instance_id))?;
                let info = ConsensusInfoResponse {
                    locked_accounts: instance.locked_accounts.clone(),
                    proposed: instance.proposed.clone(),
                    locked: instance.locked.clone(),
                    received: instance.received.clone(),
                };
                Ok(ConsensusResponse::Info(info))
            }
            ConsensusOrder::Propose {
                proposal,
                owner,
                signature,
                locks,
            } => {
                let instance = self
                    .instances
                    .get_mut(&proposal.instance_id)
                    .ok_or_else(|| Error::UnknownConsensusInstance(proposal.instance_id.clone()))?;
                // Process lock certificates.
                for lock in locks {
                    lock.check(&self.committee)?;
                    match lock.value {
                        Value::Lock(Request {
                            account_id,
                            operation: Operation::LockInto { instance_id, owner },
                            sequence_number,
                        }) if instance_id == proposal.instance_id
                            && Some(&sequence_number)
                                == instance.sequence_numbers.get(&account_id) =>
                        {
                            // Update locking status for `account_id`.
                            instance.locked_accounts.insert(account_id, owner);
                            instance.participants.insert(owner);
                        }
                        _ => return Err(Error::InvalidConsensusOrder),
                    }
                }
                // Verify the signature and that the author of the signature is authorized.
                ensure!(
                    instance.participants.contains(&owner),
                    Error::InvalidConsensusOrder
                );
                signature.check(&proposal, owner)?;
                // Check validity of the proposal and obtain the corresponding requests.
                let requests = instance.make_requests(proposal.decision)?;
                // TODO: verify that `proposal.round` is "available".
                // Verify safety.
                if let Some(proposed) = &instance.proposed {
                    ensure!(
                        (proposed.round == proposal.round
                            && proposed.decision == proposal.decision)
                            || proposed.round < proposal.round,
                        Error::UnsafeConsensusProposal
                    );
                }
                if let Some(locked) = instance
                    .locked
                    .as_ref()
                    .and_then(|cert| cert.value.pre_commit_proposal())
                {
                    ensure!(
                        locked.round < proposal.round && locked.decision == proposal.decision,
                        Error::UnsafeConsensusProposal
                    );
                }
                // Update proposed decision.
                instance.proposed = Some(proposal.clone());
                // Vote in favor of pre-commit (aka lock).
                let value = Value::PreCommit { proposal, requests };
                let vote = Vote::new(value, &self.key_pair);
                Ok(ConsensusResponse::Vote(vote))
            }
            ConsensusOrder::HandlePreCommit { certificate } => {
                certificate.check(&self.committee)?;
                let (proposal, requests) = match certificate.value.clone() {
                    Value::PreCommit { proposal, requests } => (proposal, requests),
                    _ => return Err(Error::InvalidConsensusOrder),
                };
                let instance = self
                    .instances
                    .get_mut(&proposal.instance_id)
                    .ok_or_else(|| Error::UnknownConsensusInstance(proposal.instance_id.clone()))?;
                // Verify safety.
                if let Some(proposed) = &instance.proposed {
                    ensure!(
                        proposed.round <= proposal.round,
                        Error::UnsafeConsensusPreCommit
                    );
                }
                if let Some(locked) = instance
                    .locked
                    .as_ref()
                    .and_then(|cert| cert.value.pre_commit_proposal())
                {
                    ensure!(
                        locked.round <= proposal.round,
                        Error::UnsafeConsensusPreCommit
                    );
                }
                // Update locked decision.
                instance.locked = Some(certificate);
                // Vote in favor of commit.
                let value = Value::Commit { proposal, requests };
                let vote = Vote::new(value, &self.key_pair);
                Ok(ConsensusResponse::Vote(vote))
            }
            ConsensusOrder::HandleCommit { certificate, locks } => {
                certificate.check(&self.committee)?;
                let (proposal, requests) = match &certificate.value {
                    Value::Commit { proposal, requests } => (proposal, requests),
                    _ => return Err(Error::InvalidConsensusOrder),
                };
                // Success.
                // Only execute the requests in the commit once.
                let mut requests = {
                    if self.instances.contains_key(&proposal.instance_id) {
                        requests.clone()
                    } else {
                        Vec::new()
                    }
                };
                // Process lock certificates to add skip requests if needed.
                if let ConsensusDecision::Abort = &proposal.decision {
                    for lock in locks {
                        lock.check(&self.committee)?;
                        match lock.value {
                            Value::Lock(Request {
                                account_id,
                                operation:
                                    Operation::LockInto {
                                        instance_id,
                                        owner: _,
                                    },
                                sequence_number,
                            }) if instance_id == proposal.instance_id => {
                                requests.push(Request {
                                    account_id: account_id.clone(),
                                    operation: Operation::Skip,
                                    sequence_number,
                                });
                            }
                            _ => return Err(Error::InvalidConsensusOrder),
                        }
                    }
                }
                // Remove the consensus instance if needed.
                self.instances.remove(&proposal.instance_id);
                // Prepare cross-shard requests.
                let continuations = requests
                    .iter()
                    .map(|request| {
                        let shard_id = self.which_shard(&request.account_id);
                        CrossShardContinuation::Request {
                            shard_id,
                            request: Box::new(CrossShardRequest::ProcessConfirmedRequest {
                                request: request.clone(),
                                certificate: certificate.clone(),
                            }),
                        }
                    })
                    .collect();
                Ok(ConsensusResponse::Continuations(continuations))
            }
        }
    }

    fn handle_cross_shard_request(&mut self, request: CrossShardRequest) -> Result<(), Error> {
        match request {
            CrossShardRequest::UpdateRecipient { certificate } => {
                let request = certificate
                    .value
                    .confirm_request()
                    .ok_or(Error::InvalidCrossShardRequest)?;
                self.update_recipient_account(request.operation.clone(), certificate)
            }
            CrossShardRequest::DestroyAccount { account_id } => {
                ensure!(self.in_shard(&account_id), Error::WrongShard);
                self.accounts.remove(&account_id);
                Ok(())
            }
            CrossShardRequest::ProcessConfirmedRequest {
                request,
                certificate,
            } => {
                self.process_confirmed_request(request, certificate)?; // TODO: process continuations
                Ok(())
            }
        }
    }

    fn handle_account_info_query(
        &self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error> {
        ensure!(self.in_shard(&query.account_id), Error::WrongShard);
        let account = self.account_state(&query.account_id)?;
        let mut response = account.make_account_info(query.account_id);
        if let Some(seq) = query.query_sequence_number {
            if let Some(cert) = account.confirmed_log.get(usize::from(seq)) {
                response.queried_certificate = Some(cert.clone());
            } else {
                return Err(Error::CertificateNotFound);
            }
        }
        if let Some(idx) = query.query_received_certificates_excluding_first_nth {
            response.queried_received_certificates = account.received_log[idx..].to_vec();
        }
        Ok(response)
    }
}

impl AuthorityState {
    pub fn new(committee: Committee, name: AuthorityName, key_pair: KeyPair) -> Self {
        AuthorityState {
            committee,
            name,
            key_pair,
            accounts: BTreeMap::new(),
            instances: BTreeMap::new(),
            shard_id: 0,
            number_of_shards: 1,
        }
    }

    pub fn new_shard(
        committee: Committee,
        key_pair: KeyPair,
        shard_id: u32,
        number_of_shards: u32,
    ) -> Self {
        AuthorityState {
            committee,
            name: key_pair.public(),
            key_pair,
            accounts: BTreeMap::new(),
            instances: BTreeMap::new(),
            shard_id,
            number_of_shards,
        }
    }

    pub fn in_shard(&self, account_id: &AccountId) -> bool {
        self.which_shard(account_id) == self.shard_id
    }

    pub fn get_shard(num_shards: u32, account_id: &AccountId) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut s = std::collections::hash_map::DefaultHasher::new();
        account_id.hash(&mut s);
        (s.finish() % num_shards as u64) as u32
    }

    pub fn which_shard(&self, account_id: &AccountId) -> u32 {
        Self::get_shard(self.number_of_shards, account_id)
    }

    fn account_state(&self, account_id: &AccountId) -> Result<&AccountState, Error> {
        let account = self
            .accounts
            .get(account_id)
            .ok_or_else(|| Error::InactiveAccount(account_id.clone()))?;
        ensure!(
            account.owner.is_some(),
            Error::InactiveAccount(account_id.clone())
        );
        Ok(account)
    }

    #[cfg(test)]
    pub fn accounts_mut(&mut self) -> &mut BTreeMap<AccountId, AccountState> {
        &mut self.accounts
    }
}
