// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    base_types::*, committee::Committee, consensus::ConsensusState, ensure, error::Error,
    messages::*, storage::StorageClient, AsyncResult,
};

#[cfg(test)]
#[path = "unit_tests/authority_tests.rs"]
mod authority_tests;

/// State of a worker in an authority.
pub struct WorkerState<Client> {
    /// The name of this authority.
    pub name: AuthorityName,
    /// Committee of this instance.
    pub committee: Committee,
    /// The signature key pair of the authority.
    pub key_pair: KeyPair,
    /// The sharding ID of this authority shard. 0 if one shard.
    pub shard_id: ShardId,
    /// The number of shards. 1 if single shard.
    pub number_of_shards: u32,
    /// Access to local persistent storage.
    pub storage: Client,
}

/// Interface provided by each (shard of an) authority.
/// All commands return either the current account info or an error.
/// Repeating commands produces no changes and returns no error.
pub trait Authority {
    /// Initiate a new request.
    fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> AsyncResult<AccountInfoResponse, Error>;

    /// Confirm a request.
    fn handle_confirmation_order(
        &mut self,
        order: ConfirmationOrder,
    ) -> AsyncResult<(AccountInfoResponse, CrossShardContinuation), Error>;

    /// Process a message meant for a consensus instance.
    fn handle_consensus_order(
        &mut self,
        order: ConsensusOrder,
    ) -> AsyncResult<ConsensusResponse, Error>;

    /// Handle information queries for this account.
    fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> AsyncResult<AccountInfoResponse, Error>;
}

pub trait Worker {
    /// Handle (trusted!) cross shard request.
    fn handle_cross_shard_request(&mut self, request: CrossShardRequest) -> AsyncResult<(), Error>;
}

impl<Client> WorkerState<Client>
where
    Client: StorageClient + Send + Sync,
{
    /// (Trusted) Process a confirmed request issued from an account.
    async fn process_confirmed_request(
        &mut self,
        request: Request,
        certificate: Certificate, // For logging purpose
    ) -> Result<(AccountInfoResponse, CrossShardContinuation), Error> {
        // Verify sharding.
        ensure!(self.in_shard(&request.account_id), Error::WrongShard);
        // Obtain the sender's account.
        let sender = request.account_id.clone();
        // Check that the account is active and ready for this confirmation.
        let mut account = self.storage.read_active_account(sender.clone()).await?;
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
            self.storage.remove_account(sender.clone()).await?;
        } else {
            self.storage.write_account(sender.clone(), account).await?;
        }

        if let Some(recipient) = request.operation.recipient() {
            // Update recipient.
            if self.in_shard(recipient) {
                // Execute the operation locally.
                self.update_recipient_account(request.operation.clone(), certificate)
                    .await?;
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
    async fn update_recipient_account(
        &mut self,
        operation: Operation,
        certificate: Certificate,
    ) -> Result<(), Error> {
        if let Some(recipient) = operation.recipient() {
            ensure!(self.in_shard(recipient), Error::WrongShard);
            // Execute the recipient's side of the operation.
            let mut account = self
                .storage
                .read_account_or_default(recipient.clone())
                .await?;
            account.apply_operation_as_recipient(&operation, certificate)?;
            self.storage
                .write_account(recipient.clone(), account)
                .await?;
        } else if let Operation::StartConsensusInstance {
            new_id,
            functionality: Functionality::AtomicSwap { accounts },
        } = &operation
        {
            ensure!(self.in_shard(new_id), Error::WrongShard);
            // Make sure cross shard requests to start a consensus instance cannot be replayed.
            if !self.storage.has_consensus(new_id.clone()).await? {
                let instance = ConsensusState::new(accounts.clone(), certificate);
                self.storage
                    .write_consensus(new_id.clone(), instance)
                    .await?;
            }
        }
        // This concludes the confirmation of `certificate`.
        Ok(())
    }
}

impl<Client> Authority for WorkerState<Client>
where
    Client: StorageClient + Send + Sync,
{
    fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> AsyncResult<AccountInfoResponse, Error> {
        Box::pin(async move {
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

            let mut account = self.storage.read_active_account(sender.clone()).await?;
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
            let info = account.make_account_info(sender.clone());
            self.storage.write_account(sender, account).await?;
            Ok(info)
        })
    }

    /// Confirm a request.
    fn handle_confirmation_order(
        &mut self,
        confirmation_order: ConfirmationOrder,
    ) -> AsyncResult<(AccountInfoResponse, CrossShardContinuation), Error> {
        Box::pin(async move {
            // Verify that the certified value is a confirmation.
            let certificate = confirmation_order.certificate;
            let request = certificate
                .value
                .confirm_request()
                .ok_or(Error::InvalidConfirmationOrder)?;
            // Verify the certificate.
            certificate.check(&self.committee)?;
            // Process the request.
            let r = self
                .process_confirmed_request(request.clone(), certificate)
                .await?;
            Ok(r)
        })
    }

    /// Process a message meant for a consensus instance.
    fn handle_consensus_order(
        &mut self,
        order: ConsensusOrder,
    ) -> AsyncResult<ConsensusResponse, Error> {
        Box::pin(async move {
            match order {
                ConsensusOrder::GetStatus { instance_id } => {
                    let instance = self.storage.read_consensus(instance_id).await?;
                    let info = ConsensusInfoResponse {
                        locked_accounts: instance.locked_accounts.clone(),
                        proposed: instance.proposed.clone(),
                        locked: instance.locked.clone(),
                        received: instance.received,
                    };
                    Ok(ConsensusResponse::Info(info))
                }
                ConsensusOrder::Propose {
                    proposal,
                    owner,
                    signature,
                    locks,
                } => {
                    let mut instance = self
                        .storage
                        .read_consensus(proposal.instance_id.clone())
                        .await?;
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
                    self.storage
                        .write_consensus(proposal.instance_id.clone(), instance)
                        .await?;
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
                    let mut instance = self
                        .storage
                        .read_consensus(proposal.instance_id.clone())
                        .await?;
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
                    self.storage
                        .write_consensus(proposal.instance_id.clone(), instance)
                        .await?;
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
                        if self
                            .storage
                            .has_consensus(proposal.instance_id.clone())
                            .await?
                        {
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
                    self.storage
                        .remove_consensus(proposal.instance_id.clone())
                        .await?;
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
        })
    }

    fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> AsyncResult<AccountInfoResponse, Error> {
        Box::pin(async move {
            ensure!(self.in_shard(&query.account_id), Error::WrongShard);
            let account = self
                .storage
                .read_active_account(query.account_id.clone())
                .await?;
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
        })
    }
}

impl<Client> Worker for WorkerState<Client>
where
    Client: StorageClient + Sync + Send,
{
    fn handle_cross_shard_request(&mut self, request: CrossShardRequest) -> AsyncResult<(), Error> {
        Box::pin(async move {
            match request {
                CrossShardRequest::UpdateRecipient { certificate } => {
                    let request = certificate
                        .value
                        .confirm_request()
                        .ok_or(Error::InvalidCrossShardRequest)?;
                    self.update_recipient_account(request.operation.clone(), certificate)
                        .await
                }
                CrossShardRequest::DestroyAccount { account_id } => {
                    ensure!(self.in_shard(&account_id), Error::WrongShard);
                    self.storage.remove_account(account_id);
                    Ok(())
                }
                CrossShardRequest::ProcessConfirmedRequest {
                    request,
                    certificate,
                } => {
                    self.process_confirmed_request(request, certificate).await?; // TODO: process continuations
                    Ok(())
                }
            }
        })
    }
}

/// Static shard assignment
pub fn get_shard(num_shards: u32, account_id: &AccountId) -> u32 {
    use std::hash::{Hash, Hasher};
    let mut s = std::collections::hash_map::DefaultHasher::new();
    account_id.hash(&mut s);
    (s.finish() % num_shards as u64) as u32
}

impl<Client> WorkerState<Client> {
    pub fn new(
        committee: Committee,
        name: AuthorityName,
        key_pair: KeyPair,
        shard_id: u32,
        number_of_shards: u32,
        storage: Client,
    ) -> Self {
        WorkerState {
            committee,
            name,
            key_pair,
            shard_id,
            number_of_shards,
            storage,
        }
    }

    pub fn in_shard(&self, account_id: &AccountId) -> bool {
        self.which_shard(account_id) == self.shard_id
    }

    pub fn which_shard(&self, account_id: &AccountId) -> u32 {
        get_shard(self.number_of_shards, account_id)
    }
}
