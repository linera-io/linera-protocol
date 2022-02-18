// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    base_types::*, committee::Committee, consensus::ConsensusState, ensure, error::Error,
    messages::*, storage::StorageClient,
};
use async_trait::async_trait;
use futures::future;

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
#[async_trait]
pub trait Authority {
    /// Initiate a new request.
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error>;

    /// Confirm a request.
    async fn handle_confirmation_order(
        &mut self,
        order: ConfirmationOrder,
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error>;

    /// Process a message meant for a consensus instance.
    async fn handle_consensus_order(
        &mut self,
        order: ConsensusOrder,
    ) -> Result<ConsensusResponse, Error>;

    /// Handle information queries for this account.
    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error>;
}

#[async_trait]
pub trait Worker {
    /// Handle (trusted!) cross shard request.
    async fn handle_cross_shard_request(
        &mut self,
        request: CrossShardRequest,
    ) -> Result<Vec<CrossShardRequest>, Error>;
}

impl<Client> WorkerState<Client>
where
    Client: StorageClient,
{
    /// (Trusted) Process a confirmed request issued from an account.
    async fn process_confirmed_request(
        &mut self,
        request: Request,
        certificate: Certificate, // For logging purpose
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error> {
        // Verify sharding.
        ensure!(self.in_shard(&request.account_id), Error::WrongShard);
        assert_eq!(
            &certificate.value.confirm_request().unwrap().operation,
            &request.operation
        );
        // Obtain the sender's account.
        let sender = request.account_id.clone();
        // Check that the account is active and ready for this confirmation.
        let mut account = self.storage.read_active_account(&sender).await?;
        if account.next_sequence_number < request.sequence_number {
            return Err(Error::MissingEarlierConfirmations {
                current_sequence_number: account.next_sequence_number,
            });
        }
        if account.next_sequence_number > request.sequence_number {
            // Request was already confirmed.
            let info = account.make_account_info();
            return Ok((info, Vec::new()));
        }
        // Execute the sender's side of the operation.
        account.apply_operation_as_sender(&request.operation, certificate.hash)?;
        // Advance to next sequence number.
        account.next_sequence_number.try_add_assign_one()?;
        account.pending = None;
        // Final touch on the sender's account.
        let info = account.make_account_info();
        if account.owner.is_none() {
            // Tentatively remove inactive account. (It might be created again as a
            // recipient, though. To solve this, we may implement additional cleanups in
            // the future.)
            if account.keep_sending.is_empty() {
                self.storage.remove_account(&sender).await?;
            }
            // Never send cross-shard requests from a deactivated account.
            Ok((info, Vec::new()))
        } else {
            // Persist certificate.
            self.storage.write_certificate(certificate.clone()).await?;
            // Persist account and sending to do.
            account.keep_sending.insert(certificate.hash);
            self.storage.write_account(account.clone()).await?;
            // Schedule cross-shard request if any.
            let cont = self.schedule_recipient_update(certificate.clone()).await?;
            if cont.is_empty() {
                let mut account = self.storage.read_active_account(&sender).await?;
                // Nothing to send any more.
                account.keep_sending.remove(&certificate.hash);
                self.storage.write_account(account).await?;
            }
            Ok((info, cont))
        }
    }

    /// (Trusted) Try to update the recipient account in a confirmed request.
    async fn schedule_recipient_update(
        &mut self,
        certificate: Certificate,
    ) -> Result<Vec<CrossShardRequest>, Error> {
        let operation = certificate
            .value
            .confirm_request()
            .unwrap()
            .operation
            .clone();
        if let Some(recipient) = operation.recipient() {
            // Update recipient.
            if self.in_shard(recipient) {
                // Execute the operation locally.
                self.update_recipient_account(operation, certificate)
                    .await?;
            } else {
                // Initiate a cross-shard request.
                let cont = vec![CrossShardRequest::UpdateRecipient { certificate }];
                return Ok(cont);
            }
        }
        Ok(Vec::new())
    }

    /// (Trusted) Try to update the recipient account in a confirmed request.
    async fn update_recipient_account(
        &mut self,
        operation: Operation,
        certificate: Certificate,
    ) -> Result<(), Error> {
        if let Some(recipient) = operation.recipient() {
            ensure!(self.in_shard(recipient), Error::WrongShard);
            assert_eq!(
                &certificate.value.confirm_request().unwrap().operation,
                &operation
            );
            // Execute the recipient's side of the operation.
            let mut account = self.storage.read_account_or_default(recipient).await?;
            let need_update = account.apply_operation_as_recipient(&operation, certificate.hash)?;
            if need_update {
                self.storage.write_certificate(certificate).await?;
                self.storage.write_account(account).await?;
            }
        } else if let Operation::StartConsensusInstance {
            new_id,
            functionality: Functionality::AtomicSwap { accounts },
        } = &operation
        {
            ensure!(self.in_shard(new_id), Error::WrongShard);
            // Make sure cross shard requests to start a consensus instance cannot be replayed.
            if !self.storage.has_consensus(new_id).await? {
                let instance = ConsensusState::new(new_id.clone(), accounts.clone(), certificate);
                self.storage.write_consensus(instance).await?;
            }
        }
        // This concludes the confirmation of `certificate`.
        Ok(())
    }
}

#[async_trait]
impl<Client> Authority for WorkerState<Client>
where
    Client: StorageClient + Clone + 'static,
{
    async fn handle_request_order(
        &mut self,
        order: RequestOrder,
    ) -> Result<AccountInfoResponse, Error> {
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

        let mut account = self.storage.read_active_account(&sender).await?;
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
            return Ok(account.make_account_info());
        }
        // Verify that the request is safe, and return the value of the vote.
        let value = account.validate_operation(request)?;
        let vote = Vote::new(value, &self.key_pair);
        account.pending = Some(vote);
        let info = account.make_account_info();
        self.storage.write_account(account).await?;
        Ok(info)
    }

    /// Confirm a request.
    async fn handle_confirmation_order(
        &mut self,
        confirmation_order: ConfirmationOrder,
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error> {
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
    }

    /// Process a message meant for a consensus instance.
    async fn handle_consensus_order(
        &mut self,
        order: ConsensusOrder,
    ) -> Result<ConsensusResponse, Error> {
        match order {
            ConsensusOrder::GetStatus { instance_id } => {
                let instance = self.storage.read_consensus(&instance_id).await?;
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
                let mut instance = self.storage.read_consensus(&proposal.instance_id).await?;
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
                self.storage.write_consensus(instance).await?;
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
                let mut instance = self.storage.read_consensus(&proposal.instance_id).await?;
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
                self.storage.write_consensus(instance).await?;
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
                    if self.storage.has_consensus(&proposal.instance_id).await? {
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
                self.storage.remove_consensus(&proposal.instance_id).await?;
                // Prepare cross-shard requests.
                let requests = requests
                    .iter()
                    .map(|request| CrossShardRequest::ProcessConfirmedRequest {
                        request: request.clone(),
                        certificate: certificate.clone(),
                    })
                    .collect();
                Ok(ConsensusResponse::Continuation(requests))
            }
        }
    }

    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error> {
        ensure!(self.in_shard(&query.account_id), Error::WrongShard);
        let account = self.storage.read_active_account(&query.account_id).await?;
        let mut response = account.make_account_info();
        if let Some(seq) = query.query_sequence_number {
            if let Some(key) = account.confirmed_log.get(usize::from(seq)) {
                let cert = self.storage.read_certificate(*key).await?;
                response.queried_certificate = Some(cert);
            } else {
                return Err(Error::CertificateNotFound);
            }
        }
        if let Some(idx) = query.query_received_certificates_excluding_first_nth {
            let tasks = account.received_log[idx..].iter().map(|key| {
                let key = *key;
                let mut client = self.storage.clone();
                tokio::task::spawn(async move { client.read_certificate(key).await })
            });
            let results = future::join_all(tasks).await;
            let mut certs = Vec::new();
            for result in results {
                certs.push(result.expect("task should not cancel or crash")?);
            }
            response.queried_received_certificates = certs;
        }
        Ok(response)
    }
}

#[async_trait]
impl<Client> Worker for WorkerState<Client>
where
    Client: StorageClient,
{
    async fn handle_cross_shard_request(
        &mut self,
        request: CrossShardRequest,
    ) -> Result<Vec<CrossShardRequest>, Error> {
        match request {
            CrossShardRequest::UpdateRecipient { certificate } => {
                let request = certificate
                    .value
                    .confirm_request()
                    .ok_or(Error::InvalidCrossShardRequest)?;
                let sender = request.account_id.clone();
                let hash = certificate.hash;
                self.update_recipient_account(request.operation.clone(), certificate)
                    .await?;
                if self.in_shard(&sender) {
                    Ok(Vec::new())
                } else {
                    // Reply with a cross-shard request.
                    let cont = vec![CrossShardRequest::ConfirmUpdatedRecipient {
                        account_id: sender,
                        hash,
                    }];
                    Ok(cont)
                }
            }
            CrossShardRequest::DestroyAccount { account_id } => {
                ensure!(self.in_shard(&account_id), Error::WrongShard);
                self.storage.remove_account(&account_id).await?;
                Ok(Vec::new())
            }
            CrossShardRequest::ProcessConfirmedRequest {
                request,
                certificate,
            } => {
                self.process_confirmed_request(request, certificate).await?; // TODO: process continuations
                Ok(Vec::new())
            }
            CrossShardRequest::ConfirmUpdatedRecipient { account_id, hash } => {
                ensure!(self.in_shard(&account_id), Error::WrongShard);
                let mut account = self.storage.read_active_account(&account_id).await?;
                if account.keep_sending.remove(&hash) {
                    self.storage.write_account(account).await?;
                }
                Ok(Vec::new())
            }
        }
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
