// Copyright (c) Facebook, Inc. and its affiliates.
// Copyright (c) Zefchain Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    account::Outcome, base_types::*, committee::Committee, ensure, error::Error, messages::*,
    storage::StorageClient,
};
use async_trait::async_trait;

#[cfg(test)]
#[path = "unit_tests/authority_tests.rs"]
mod authority_tests;

/// State of a worker in an authority or a client.
pub struct WorkerState<StorageClient> {
    /// The name of this authority.
    pub name: AuthorityName,
    /// Committee of this instance.
    pub committee: Committee,
    /// The signature key pair of the authority. The key may be missing for replicas
    /// without voting rights (possibly with a partial view of accounts).
    pub key_pair: Option<KeyPair>,
    /// Access to local persistent storage.
    pub storage: StorageClient,
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

    /// Process a certificate, for instance to execute a confirmed request.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error>;

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
    Client: StorageClient + Clone + 'static,
{
    /// (Trusted) Process a confirmed request issued from an account.
    async fn process_confirmed_request(
        &mut self,
        request: Request,
        certificate: Certificate, // For logging purpose
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error> {
        assert_eq!(
            &certificate.value.confirmed_request().unwrap().operation,
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
        // Load pending cross-shard requests.
        let mut continuation = self
            .storage
            .read_certificates(account.keep_sending.iter().cloned())
            .await?
            .into_iter()
            .map(|certificate| CrossShardRequest::UpdateRecipient { certificate })
            .collect();
        if account.next_sequence_number > request.sequence_number {
            // Request was already confirmed.
            let info = account.make_account_info();
            return Ok((info, continuation));
        }
        // Persist certificate.
        self.storage.write_certificate(certificate.clone()).await?;
        // Execute the sender's side of the operation.
        account.apply_operation_as_sender(&request.operation, certificate.hash)?;
        // Advance to next sequence number.
        account.next_sequence_number.try_add_assign_one()?;
        account.manager.reset();
        // Final touch on the sender's account.
        let info = account.make_account_info();
        if !account.manager.is_active() {
            // Tentatively remove inactive account. (It might be created again as a
            // recipient, though. To solve this, we may implement additional cleanups in
            // the future.)
            if account.keep_sending.is_empty() {
                self.storage.remove_account(&sender).await?;
                // Never send cross-shard requests from a deactivated account.
                assert!(continuation.is_empty());
            }
        } else {
            // Schedule cross-shard request if any.
            let operation = &certificate.value.confirmed_request().unwrap().operation;
            if operation.recipient().is_some() {
                // Schedule a new cross-shard request to update recipient.
                account.keep_sending.insert(certificate.hash);
                continuation.push(CrossShardRequest::UpdateRecipient { certificate });
            }
            // Persist account.
            self.storage.write_account(account.clone()).await?;
        }
        Ok((info, continuation))
    }

    /// (Trusted) Process a validated request issued from a multi-owner account.
    async fn process_validated_request(
        &mut self,
        request: Request,
        round: RoundNumber,
        certificate: Certificate,
    ) -> Result<AccountInfoResponse, Error> {
        assert_eq!(certificate.value.validated_request().unwrap(), &request);
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
            return Ok(account.make_account_info());
        }
        if account.manager.check_validated_request(&request, round)? == Outcome::Skip {
            // If we just processed the same pending request, return the account info
            // unchanged.
            return Ok(account.make_account_info());
        }
        account
            .manager
            .create_final_vote(request, certificate, self.key_pair.as_ref());
        let info = account.make_account_info();
        self.storage.write_account(account).await?;
        Ok(info)
    }

    /// (Trusted) Try to update the recipient account in a confirmed request.
    async fn update_recipient_account(
        &mut self,
        operation: Operation,
        certificate: Certificate,
    ) -> Result<(), Error> {
        if let Some(recipient) = operation.recipient() {
            assert_eq!(
                &certificate.value.confirmed_request().unwrap().operation,
                &operation
            );
            // Execute the recipient's side of the operation.
            let mut account = self.storage.read_account_or_default(recipient).await?;
            let need_update = account.apply_operation_as_recipient(&operation, certificate.hash)?;
            if need_update {
                self.storage.write_certificate(certificate).await?;
                self.storage.write_account(account).await?;
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
        // Verify that the order was meant for this authority.
        if let Some(authority) = &order.value.limited_to {
            ensure!(self.name == *authority, Error::InvalidRequestOrder);
        }
        // Obtain the sender's account.
        let sender = order.value.request.account_id.clone();
        let mut account = self.storage.read_active_account(&sender).await?;
        // Check authentication of the request.
        order.check(&account.manager)?;
        // Check the account is ready for this new request.
        let request = &order.value.request;
        let round = order.value.round;
        let number = request.sequence_number;
        ensure!(
            number <= SequenceNumber::max(),
            Error::InvalidSequenceNumber
        );
        ensure!(
            number == account.next_sequence_number,
            Error::UnexpectedSequenceNumber
        );
        // Check the well-formedness of the request.
        if account.manager.check_request(request, round)? == Outcome::Skip {
            // If we just processed the same pending request, return the account info
            // unchanged.
            return Ok(account.make_account_info());
        }
        // Verify that the request is valid.
        account.validate_operation(request)?;
        // Create the vote and store it in the account.
        account.manager.create_vote(order, self.key_pair.as_ref());
        let info = account.make_account_info();
        self.storage.write_account(account).await?;
        Ok(info)
    }

    /// Process a certificate.
    async fn handle_certificate(
        &mut self,
        certificate: Certificate,
    ) -> Result<(AccountInfoResponse, Vec<CrossShardRequest>), Error> {
        // Verify the certificate.
        certificate.check(&self.committee)?;
        // Process the order.
        match &certificate.value {
            Value::Validated { request, round } => {
                // Confirm the validated request.
                let info = self
                    .process_validated_request(request.clone(), *round, certificate)
                    .await?;
                Ok((info, Vec::new()))
            }
            Value::Confirmed { request } => {
                // Execute the confirmed request.
                self.process_confirmed_request(request.clone(), certificate)
                    .await
            }
        }
    }

    async fn handle_account_info_query(
        &mut self,
        query: AccountInfoQuery,
    ) -> Result<AccountInfoResponse, Error> {
        let account = self.storage.read_active_account(&query.account_id).await?;
        let mut response = account.make_account_info();
        if let Some(range) = query.query_sent_certificates_in_range {
            let keys = account.confirmed_log[..]
                .iter()
                .skip(range.start.into())
                .cloned();
            let certs = match range.limit {
                None => self.storage.read_certificates(keys).await?,
                Some(count) => self.storage.read_certificates(keys.take(count)).await?,
            };
            response.queried_sent_certificates = certs;
        }
        if let Some(idx) = query.query_received_certificates_excluding_first_nth {
            let keys = account.received_log[..].iter().skip(idx).cloned();
            let certs = self.storage.read_certificates(keys).await?;
            response.queried_received_certificates = certs;
        }
        Ok(response)
    }
}

#[async_trait]
impl<Client> Worker for WorkerState<Client>
where
    Client: StorageClient + Clone + 'static,
{
    async fn handle_cross_shard_request(
        &mut self,
        request: CrossShardRequest,
    ) -> Result<Vec<CrossShardRequest>, Error> {
        match request {
            CrossShardRequest::UpdateRecipient { certificate } => {
                let request = certificate
                    .value
                    .confirmed_request()
                    .ok_or(Error::InvalidCrossShardRequest)?;
                let sender = request.account_id.clone();
                let hash = certificate.hash;
                self.update_recipient_account(request.operation.clone(), certificate)
                    .await?;
                // Reply with a cross-shard request.
                let cont = vec![CrossShardRequest::ConfirmUpdatedRecipient {
                    account_id: sender,
                    hash,
                }];
                Ok(cont)
            }
            CrossShardRequest::ConfirmUpdatedRecipient { account_id, hash } => {
                let mut account = self.storage.read_active_account(&account_id).await?;
                if account.keep_sending.remove(&hash) {
                    self.storage.write_account(account).await?;
                }
                Ok(Vec::new())
            }
        }
    }
}

impl<Client> WorkerState<Client> {
    pub fn new(
        committee: Committee,
        name: AuthorityName,
        key_pair: Option<KeyPair>,
        storage: Client,
    ) -> Self {
        WorkerState {
            committee,
            name,
            key_pair,
            storage,
        }
    }
}

#[cfg(test)]
pub async fn fully_handle_certificate<Client>(
    state: &mut WorkerState<Client>,
    certificate: Certificate,
) -> Result<AccountInfoResponse, crate::error::Error>
where
    Client: StorageClient + Clone + 'static,
{
    let (info, mut requests) = state.handle_certificate(certificate).await?;
    while let Some(request) = requests.pop() {
        requests.extend(state.handle_cross_shard_request(request).await?);
    }
    let account = state.storage.read_active_account(&info.account_id).await?;
    let info = account.make_account_info();
    Ok(info)
}
